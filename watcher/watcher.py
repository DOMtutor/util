import abc
import asyncio
import enum
import logging
import os
import time
from typing import Collection, Tuple, List

import aiohttp
from aiohttp import BasicAuth
import aiogram
from slack_sdk.errors import SlackClientError
from slack_sdk.web.async_client import AsyncWebClient as SlackWebClient
from dateutil import parser

judge_url = "https://domjudge.ist.ac.at"
api_url = f"{judge_url}/api/v4"


class EventType(enum.Enum):
    JudgeStatus = 0
    JudgeHostStatus = 1
    NewClarification = 2


def truncate_text(string: str, total_length=200, line_count=5):
    lines = [line for line in string.splitlines(keepends=False) if line and not line.startswith(">")]
    if len(lines) > line_count:
        lines = lines[:line_count]
        lines_truncated = True
    else:
        lines_truncated = False
    string = "\n".join(lines)
    if len(string) >= total_length:
        return string[:total_length] + "..."
    if lines_truncated:
        return string + "..."
    return string


class Channel(abc.ABC):
    async def check_auth(self) -> bool:
        pass

    @abc.abstractmethod
    async def send_message(self, message: str) -> bool:
        pass


class SlackChannel(Channel):
    def __init__(self, slack_client: SlackWebClient, channel: str):
        self.slack_client = slack_client
        self.channel = channel

    async def check_auth(self):
        try:
            await self.slack_client.auth_test()
        except SlackClientError as e:
            logging.error("Cannot authenticate with given token: %s", e)
            return False
        return True

    async def send_message(self, message: str):
        try:
            await self.slack_client.chat_postMessage(channel=self.channel, text=message)
        except SlackClientError as e:
            logging.warning("Failed to post message to Slack: %s", e)
            return False
        return True


class TelegramChannel(Channel):
    def __init__(self, telegram_bot: aiogram.Bot, chat_ids: List[str]):
        self.telegram_bot = telegram_bot
        self.chat_ids = chat_ids

    async def check_auth(self) -> bool:
        return await self.telegram_bot.get_me() is not None

    async def send_message(self, message: str) -> bool:
        try:
            await asyncio.gather(*[self.telegram_bot.send_message(chat_id=chat_id, text=message)
                                   for chat_id in self.chat_ids])
        except aiogram.exceptions.TelegramAPIError as e:
            logging.warning("Failed to post message to Telegram: %s", e)
            return False
        return True


class Watcher(object):
    def __init__(self, judge_session: aiohttp.ClientSession, channels: List[Tuple[Channel, Collection[EventType]]]):
        self.judge_session = judge_session
        self.last_clarification_update = time.time()
        self.channels = channels
        self.event_channels = {event: [] for event in EventType}
        for channel, events in self.channels:
            for event in set(events):
                self.event_channels[event].append(channel)

        self.judge_status = True
        self.hosts_status = True

    async def send_event(self, event_type: EventType, message: str):
        delivery = [channel.send_message(message) for channel in self.event_channels[event_type]]
        return all(await asyncio.gather(*delivery))

    async def update_judge_status(self, status, message):
        if status == self.judge_status:
            return
        self.judge_status = status
        await self.send_event(EventType.JudgeStatus, message)

    async def check_judge_api_status(self):
        try:
            status_request = await self.judge_session.get(f"{api_url}/")
            if status_request.ok:
                await self.update_judge_status(True, f"Reconnected to judge")
            else:
                await self.update_judge_status(False, f"Failed to connect to judge: {status_request.status}")
        except aiohttp.ClientError as e:
            await self.update_judge_status(False, f"Failed to connect to judge: {e}")

    async def request_from_judge_api(self, suffix, **kwargs):
        url = f"{api_url}{suffix}"
        try:
            response = await self.judge_session.get(url, **kwargs)

            if not response.ok:
                logging.debug("Error during request %s: %d", url, response.status)
                await self.update_judge_status(False, f"Failed to query judge {url}: {response.status}")
                return None
            return await response.json()
        except aiohttp.ClientError as e:
            logging.debug("Error during request %s: %s", url, e)
            return None

    async def query_judge_host_status(self):
        judge_hosts = await self.request_from_judge_api("/judgehosts")
        if judge_hosts is None:
            return
        if not judge_hosts:
            if self.hosts_status is not None:
                self.hosts_status = None
                await self.send_event(EventType.JudgeStatus, "Judges are down!")
            return
        if all(not judge["active"] or time.time() - judge["polltime"] < 30 for judge in judge_hosts):
            if not self.hosts_status:
                await self.send_event(EventType.JudgeStatus, "All judges are up.")
            self.hosts_status = True
        elif self.hosts_status:
            await self.send_event(EventType.JudgeStatus, "Some judges went offline.")
            self.hosts_status = False

    async def query_clarifications(self):
        contests = await self.request_from_judge_api("/contests")
        if not contests:
            return

        contest_ids = []
        for contest in contests:
            contest_ids.append(contest["id"])

        if not contest_ids:
            logging.debug("No active contest")
            return

        requests = [asyncio.create_task(self.request_from_judge_api(f"/contests/{contest_id}/clarifications"))
                    for contest_id in contest_ids]
        clarifications = []
        while requests:
            finished, requests = await asyncio.wait(requests, return_when=asyncio.FIRST_COMPLETED)
            for task in finished:
                if task.result() is None:
                    return
                clarifications.extend(task.result())
        answered = set()
        new_team_clarifications = []
        for clarification in clarifications:
            reply = clarification["reply_to_id"]
            if reply:
                answered.add(reply)
            if self.last_clarification_update < parser.parse(clarification["time"]).timestamp() and \
                    clarification.get("from_team_id"):
                new_team_clarifications.append(clarification)

        if new_team_clarifications:
            clarification_texts = []
            for clarification in new_team_clarifications:
                if clarification["id"] in answered:
                    continue
                text = ""
                # TODO Query problem & team names from judge?
                # text = clarification['from_team_id']
                # if "problem_id" in clarification:
                #    text += f"({clarification['problem_id']})"
                text += f"\n{truncate_text(clarification['text'])}\n"
                text += f"Link: {judge_url}/jury/clarifications/{clarification['id']}"
                clarification_texts.append(text)

            message = "New unanswered clarification requests:\n" + "\n\n".join(clarification_texts)
            if await self.send_event(EventType.NewClarification, message):
                self.last_clarification_update = time.time()

    async def run(self):
        if not all(await asyncio.gather(*[channel.check_auth() for channel, _ in self.channels])):
            return

        while True:
            await asyncio.gather(*[
                self.check_judge_api_status(),
                self.query_clarifications(),
                self.query_judge_host_status()
            ])
            await asyncio.sleep(60)


async def main():
    slack_client = SlackWebClient(token=os.environ["SLACK_API_TOKEN"])
    telegram_bot = aiogram.Bot(token=os.environ["TELEGRAM_BOT_TOKEN"])
    async with aiohttp.ClientSession(auth=BasicAuth('python_bot', os.environ["DOMJUDGE_API_PASSWORD"])) as session:
        await Watcher(session, [
            (SlackChannel(slack_client, "C02JK69916D"), [EventType.NewClarification]),
            (TelegramChannel(telegram_bot, ["759428835"]), [EventType.JudgeStatus])
        ]).run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.get_event_loop().run_until_complete(main())
