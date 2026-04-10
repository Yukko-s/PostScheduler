import io
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from dotenv import load_dotenv

import job_scheduler
import message_parser

load_dotenv()

JST = ZoneInfo("Asia/Tokyo")
SCHEDULE_CHANNEL_NAME = os.getenv("SCHEDULE_CHANNEL_NAME", "予約投稿")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PostScheduler")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = discord.Client(intents=intents)


# ------------------------------------------------------------------ helpers

def _job_id(message_id: int) -> str:
    return f"schedule_{message_id}"


def _is_schedule_channel(channel) -> bool:
    return (
        isinstance(channel, discord.TextChannel)
        and channel.name == SCHEDULE_CHANNEL_NAME
    )


# ------------------------------------------------------------------ post logic

async def _post_scheduled(schedule_channel_id: int, message_id: int):
    """APSchedulerから呼ばれる。メッセージを取得して投稿先に送信する。"""
    logger.info(f"_post_scheduled 呼び出し: message_id={message_id}")
    try:
        schedule_channel = bot.get_channel(schedule_channel_id)
        if not schedule_channel:
            logger.error(f"予約投稿チャンネル {schedule_channel_id} が見つかりません")
            return

        # 投稿時点でメッセージを取得（最新の内容を使う）
        try:
            message = await schedule_channel.fetch_message(message_id)
        except discord.NotFound:
            logger.warning(f"メッセージ {message_id} が削除済みのためスキップします")
            return

        parsed, error = message_parser.parse(message.content)
        if error or not parsed:
            logger.error(f"投稿時のパースエラー (message_id={message_id}): {error}")
            return

        target_channel = bot.get_channel(parsed.target_channel_id)
        if not target_channel:
            logger.error(f"投稿先チャンネル {parsed.target_channel_id} が見つかりません")
            try:
                await message.remove_reaction("⏳", bot.user)
            except Exception:
                pass
            await message.add_reaction("❌")
            await message.reply("⚠️ 投稿先チャンネルが見つかりません", mention_author=False)
            return

        if message.attachments:
            files = [
                discord.File(io.BytesIO(await a.read()), filename=a.filename)
                for a in message.attachments
            ]
            await target_channel.send(files=files)

        if parsed.body:
            await target_channel.send(parsed.body)
        logger.info(f"投稿完了: message_id={message_id} → #{target_channel.name}")

        if parsed.repeat:
            # 繰り返しジョブ: 次回をスケジュールしてピン留め継続
            now = datetime.now(JST)
            next_dt = message_parser.calc_next_dt(parsed, now)
            if next_dt:
                job_scheduler.add_job(
                    _job_id(message_id),
                    _post_scheduled,
                    run_date=next_dt,
                    args=[schedule_channel_id, message_id],
                )
                logger.info(f"次回スケジュール: {next_dt}")
        else:
            # 単発ジョブ: ✅をつけてピン留め解除
            try:
                await message.remove_reaction("⏳", bot.user)
            except Exception:
                pass
            await message.add_reaction("✅")
            await message.unpin()

    except Exception:
        logger.exception(f"_post_scheduled でエラーが発生しました (message_id={message_id})")
        try:
            schedule_channel = bot.get_channel(schedule_channel_id)
            if schedule_channel:
                msg = await schedule_channel.fetch_message(message_id)
                await msg.remove_reaction("⏳", bot.user)
                await msg.add_reaction("❌")
                await msg.reply("⚠️ 投稿中にエラーが発生しました", mention_author=False)
        except Exception:
            pass


# ------------------------------------------------------------------ registration

async def _register_message(message: discord.Message):
    """メッセージをパースしてジョブ登録・リアクション付与・ピン留めを行う。"""
    parsed, error = message_parser.parse(message.content)

    if error or not parsed:
        await message.add_reaction("❌")
        await message.reply(f"⚠️ フォーマットエラー\n{error}", mention_author=False)
        return

    now = datetime.now(JST)
    run_date = parsed.scheduled_at

    # 日時が過去の場合の処理
    if run_date <= now:
        if parsed.repeat:
            # 繰り返しジョブなら次回を計算
            run_date = message_parser.calc_next_dt(parsed, now)
            if not run_date:
                await message.add_reaction("❌")
                await message.reply("⚠️ 次回の実行日時を計算できませんでした", mention_author=False)
                return
        else:
            await message.add_reaction("❌")
            await message.reply("⚠️ 指定した日時は過去です", mention_author=False)
            return

    job_scheduler.add_job(
        _job_id(message.id),
        _post_scheduled,
        run_date=run_date,
        args=[message.channel.id, message.id],
    )

    await message.add_reaction("⏳")
    await message.pin()
    logger.info(f"予約登録: message_id={message.id}, run_date={run_date}")


# ------------------------------------------------------------------ events

@bot.event
async def on_ready():
    logger.info(f"起動しました: {bot.user} (ID: {bot.user.id})")
    job_scheduler.start()

    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=SCHEDULE_CHANNEL_NAME)
        if not ch:
            logger.warning(f"#{SCHEDULE_CHANNEL_NAME} が {guild.name} に見つかりません")
            continue

        try:
            pins = [msg async for msg in ch.pins()]
        except discord.Forbidden:
            logger.warning(f"#{SCHEDULE_CHANNEL_NAME} へのアクセス権限がありません ({guild.name})")
            continue

        now = datetime.now(JST)
        count = 0

        for msg in pins:
            # ⏳がついているもの = 未投稿の予約
            if not any(str(r.emoji) == "⏳" for r in msg.reactions):
                continue

            parsed, error = message_parser.parse(msg.content)
            if error or not parsed:
                continue

            run_date = parsed.scheduled_at
            if run_date <= now:
                if parsed.repeat:
                    run_date = message_parser.calc_next_dt(parsed, now)
                    if not run_date:
                        continue
                else:
                    # 再起動中に単発ジョブの時間を過ぎていた場合
                    logger.warning(f"単発ジョブの時間を過ぎています: message_id={msg.id}")
                    try:
                        await msg.remove_reaction("⏳", bot.user)
                        await msg.add_reaction("❌")
                        await msg.unpin()
                        await msg.reply(
                            "⚠️ Bot停止中に投稿時刻を過ぎたため、投稿できませんでした",
                            mention_author=False,
                        )
                    except Exception:
                        pass
                    continue

            job_scheduler.add_job(
                _job_id(msg.id),
                _post_scheduled,
                run_date=run_date,
                args=[ch.id, msg.id],
            )
            count += 1

        logger.info(f"{guild.name} の #{SCHEDULE_CHANNEL_NAME} から {count} 件のジョブを復元しました")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if _is_schedule_channel(message.channel):
        if message.content.strip().startswith("日時:"):
            await _register_message(message)


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if after.author.bot:
        return
    if not _is_schedule_channel(after.channel):
        return

    # 内容が変わっていない場合（ピン留めなどによる編集イベント）は無視
    if before.content == after.content:
        return

    # ⏳がついているメッセージの編集のみ処理
    if not any(str(r.emoji) == "⏳" for r in before.reactions):
        return

    # 古いリアクションをクリア
    for emoji in ("⏳", "❌"):
        try:
            await after.remove_reaction(emoji, bot.user)
        except Exception:
            pass

    # 古いジョブを削除して再登録
    job_scheduler.remove_job(_job_id(after.id))
    await _register_message(after)
    logger.info(f"予約を更新しました: message_id={after.id}")


@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    if not _is_schedule_channel(message.channel):
        return

    jid = _job_id(message.id)
    if job_scheduler.has_job(jid):
        job_scheduler.remove_job(jid)
        logger.info(f"予約をキャンセルしました: message_id={message.id}")


# ------------------------------------------------------------------ run

bot.run(os.getenv("DISCORD_TOKEN"))
