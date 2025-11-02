"""
AstrBot 群发言统计插件
统计群成员发言次数，生成排行榜
"""

import asyncio
import json
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from cachetools import TTLCache

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, MessageChain
from astrbot.api.event.filter import EventMessageType
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger as astrbot_logger
import astrbot.api.message_components as Comp
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent

from .utils.data_manager import DataManager
from .utils.image_generator import ImageGenerator, ImageGenerationError
from .utils.validators import Validators, ValidationError, CommandValidator
from .utils.models import (
    UserData, PluginConfig, GroupInfo, MessageDate, 
    RankType
)




@register("message_stats", "xiaoruange39", "群发言统计插件", "1.0")
class MessageStatsPlugin(Star):
    """群发言统计插件"""
    
    def __init__(self, context: Context, config = None):
        super().__init__(context)
        self.logger = astrbot_logger
        
        # 使用StarTools获取插件数据目录
        data_dir = StarTools.get_data_dir('message_stats')
        
        # 初始化组件
        self.data_manager = DataManager(data_dir)
        
        # 插件配置将在初始化时从DataManager获取
        self.plugin_config = None
        self.image_generator = None
        
        # 群成员列表缓存 - 5分钟TTL，减少API调用
        self.group_members_cache = TTLCache(maxsize=100, ttl=300)
        
        # 插件状态
        self.initialized = False
    
    async def initialize(self):
        """插件初始化"""
        try:
            self.logger.info("群发言统计插件初始化中...")
            
            # 初始化数据管理器
            await self.data_manager.initialize()
            
            # 从DataManager获取插件配置（确保config.json存在，如果不存在则创建默认配置）
            self.plugin_config = await self.data_manager.get_config()
            
            # 创建图片生成器
            self.image_generator = ImageGenerator(self.plugin_config)
            
            # 初始化图片生成器
            try:
                await self.image_generator.initialize()
                self.logger.info("图片生成器初始化成功")
            except ImageGenerationError as e:
                self.logger.warning(f"图片生成器初始化失败: {e}")
            
            self.initialized = True
            self.logger.info("群发言统计插件初始化完成")
            
        except Exception as e:
            self.logger.error(f"插件初始化失败: {e}")
            raise
    
    async def terminate(self):
        """插件卸载清理"""
        try:
            self.logger.info("群发言统计插件卸载中...")
            
            # 清理图片生成器
            if self.image_generator:
                await self.image_generator.cleanup()
            
            # 清理数据缓存
            await self.data_manager.clear_cache()
            
            # 清理群成员列表缓存
            self.group_members_cache.clear()
            self.logger.info("群成员列表缓存已清理")
            
            self.initialized = False
            self.logger.info("群发言统计插件卸载完成")
            
        except Exception as e:
            self.logger.error(f"插件卸载失败: {e}")
    
    # ========== 消息监听 ==========
    
    @filter.event_message_type(EventMessageType.ALL)
    async def auto_message_listener(self, event: AstrMessageEvent):
        """自动消息监听器 - 监听所有消息并记录群成员发言统计"""
        try:
            # 获取消息字符串
            message_str = getattr(event, 'message_str', None)
            
            # 跳过命令消息
            if message_str and (message_str.startswith('%') or message_str.startswith('/')):
                return
            
            # 获取群ID
            try:
                group_id = event.get_group_id()
            except Exception as e:
                group_id = None
            
            # 如果不是群聊消息，跳过
            if not group_id:
                return
            
            # 获取用户ID
            try:
                user_id = event.get_sender_id()
            except Exception as e:
                user_id = None
            
            if not user_id:
                return
            
            # 转换为字符串
            group_id = str(group_id)
            user_id = str(user_id)
            
            # 跳过机器人自身消息
            try:
                self_id = event.get_self_id()
                if self_id and user_id == str(self_id):
                    return
            except Exception as e:
                pass
            
            # 获取用户昵称（优先使用群昵称）
            nickname = await self._get_user_display_name(event, group_id, user_id)
            
            # 记录消息统计
            await self._record_message_stats(group_id, user_id, nickname)
            
        except Exception as e:
            self.logger.error(f"自动消息监听失败: {e}")
    
    async def _record_message_stats(self, group_id: str, user_id: str, nickname: str):
        """记录消息统计"""
        try:
            # 验证数据
            group_id = Validators.validate_group_id(group_id)
            user_id = Validators.validate_user_id(user_id)
            nickname = Validators.validate_nickname(nickname)
            
            # 获取当前日期
            today = date.today()
            
            # 直接使用data_manager更新用户消息
            success = await self.data_manager.update_user_message(group_id, user_id, nickname)
            
            if success:
                self.logger.debug(f"消息统计记录成功: {nickname}")
            else:
                self.logger.error(f"消息统计记录失败: {nickname}")
            
        except Exception as e:
            self.logger.error(f"记录消息统计失败: {e}")
    
    # ========== 排行榜命令 ==========
    
    @filter.command("更新发言统计")
    async def update_message_stats(self, event: AstrMessageEvent):
        """手动更新发言统计"""
        try:
            # 使用AstrBot官方API获取群组ID和用户ID
            group_id = event.get_group_id()
            user_id = event.get_sender_id()
            
            if not group_id:
                yield event.plain_result("无法获取群组信息，请在群聊中使用此命令！")
                return
                
            if not user_id:
                yield event.plain_result("无法获取用户信息！")
                return
            
            group_id = str(group_id)
            user_id = str(user_id)
            
            # 获取用户显示名称（优先使用群昵称）
            user_name = await self._get_user_display_name(event, group_id, user_id)
            
            # 记录当前用户的发言
            await self.data_manager.update_user_message(group_id, user_id, user_name)
            
            yield event.plain_result(f"已记录 {user_name} 的发言统计！")
            
        except Exception as e:
            self.logger.error(f"更新发言统计失败: {e}")
            yield event.plain_result("更新发言统计失败，请稍后重试")
    
    @filter.command("发言榜")
    async def show_full_rank(self, event: AstrMessageEvent):
        """显示总排行榜"""
        async for result in self._show_rank(event, RankType.TOTAL):
            yield result
    
    @filter.command("今日发言榜")
    async def show_daily_rank(self, event: AstrMessageEvent):
        """显示今日排行榜"""
        async for result in self._show_rank(event, RankType.DAILY):
            yield result
    
    @filter.command("本周发言榜")
    async def show_weekly_rank(self, event: AstrMessageEvent):
        """显示本周排行榜"""
        async for result in self._show_rank(event, RankType.WEEKLY):
            yield result
    
    @filter.command("本月发言榜")
    async def show_monthly_rank(self, event: AstrMessageEvent):
        """显示本月排行榜"""
        async for result in self._show_rank(event, RankType.MONTHLY):
            yield result
    
    # ========== 设置命令 ==========
    
    @filter.command("设置发言榜数量")
    async def set_rank_count(self, event: AstrMessageEvent):
        """设置排行榜显示人数"""
        try:
            # 获取群组ID
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息，请在群聊中使用此命令！")
                return
            
            group_id = str(group_id)
            
            # 获取参数
            command_validator = CommandValidator()
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            if not args:
                yield event.plain_result("请指定数量！用法：#设置发言榜数量 10")
                return
            
            # 验证数量
            try:
                count = int(args[0])
                if count <= 0 or count > 100:
                    yield event.plain_result("数量必须在1-100之间！")
                    return
            except ValueError:
                yield event.plain_result("数量必须是数字！")
                return
            
            # 保存配置
            config = await self.data_manager.get_config()
            config.rand = count
            await self.data_manager.save_config(config)
            
            yield event.plain_result(f"排行榜显示人数已设置为 {count} 人！")
            
        except Exception as e:
            self.logger.error(f"设置排行榜数量失败: {e}")
            yield event.plain_result("设置失败，请稍后重试")
    
    @filter.command("设置发言榜图片")
    async def set_image_mode(self, event: AstrMessageEvent):
        """设置图片模式"""
        try:
            # 获取群组ID
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息，请在群聊中使用此命令！")
                return
            
            group_id = str(group_id)
            
            # 获取参数
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            if not args:
                yield event.plain_result("请指定模式！用法：#设置发言榜图片 1")
                return
            
            # 验证模式
            mode = args[0].lower()
            if mode in ['1', 'true', '开', 'on', 'yes']:
                if_send_pic = 1
                mode_text = "图片模式"
            elif mode in ['0', 'false', '关', 'off', 'no']:
                if_send_pic = 0
                mode_text = "文字模式"
            else:
                yield event.plain_result("模式参数错误！可用：1/true/开 或 0/false/关")
                return
            
            # 保存配置
            config = await self.data_manager.get_config()
            config.if_send_pic = if_send_pic
            await self.data_manager.save_config(config)
            
            yield event.plain_result(f"排行榜显示模式已设置为 {mode_text}！")
            
        except Exception as e:
            self.logger.error(f"设置图片模式失败: {e}")
            yield event.plain_result("设置失败，请稍后重试")
    
    @filter.command("清除发言榜单")
    async def clear_message_ranking(self, event: AstrMessageEvent):
        """清除发言榜单"""
        try:
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息，请在群聊中使用此命令！")
                return
            group_id = str(group_id)
            
            success = await self.data_manager.clear_group_data(group_id)
            
            if success:
                yield event.plain_result("本群发言榜单已清除！")
            else:
                yield event.plain_result("清除榜单失败，请稍后重试！")
            
        except Exception as e:
            self.logger.error(f"清除榜单失败: {e}")
            yield event.plain_result("清除榜单失败，请稍后重试！")
    
    @filter.command("刷新群成员缓存")
    async def refresh_group_members_cache(self, event: AstrMessageEvent):
        """刷新群成员列表缓存"""
        try:
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息，请在群聊中使用此命令！")
                return
            group_id = str(group_id)
            
            # 清除特定群的成员缓存
            cache_key = f"group_members_{group_id}"
            if cache_key in self.group_members_cache:
                del self.group_members_cache[cache_key]
                self.logger.info(f"已清除群 {group_id} 的成员缓存")
                yield event.plain_result("群成员缓存已刷新！")
            else:
                yield event.plain_result("该群没有缓存的成员信息！")
            
        except Exception as e:
            self.logger.error(f"刷新群成员缓存失败: {e}")
            yield event.plain_result("刷新缓存失败，请稍后重试！")
    
    @filter.command("缓存状态")
    async def show_cache_status(self, event: AstrMessageEvent):
        """显示缓存状态"""
        try:
            # 获取数据管理器缓存统计
            cache_stats = await self.data_manager.get_cache_stats()
            
            # 获取群成员缓存信息
            members_cache_size = len(self.group_members_cache)
            members_cache_maxsize = self.group_members_cache.maxsize
            
            status_msg = [
                "📊 缓存状态报告",
                "━━━━━━━━━━━━━━",
                f"💾 数据缓存: {cache_stats['data_cache_size']}/{cache_stats['data_cache_maxsize']}",
                f"⚙️ 配置缓存: {cache_stats['config_cache_size']}/{cache_stats['config_cache_maxsize']}",
                f"👥 群成员缓存: {members_cache_size}/{members_cache_maxsize}",
                "━━━━━━━━━━━━━━",
                "🕐 数据缓存TTL: 5分钟",
                "🕐 配置缓存TTL: 1分钟", 
                "🕐 群成员缓存TTL: 5分钟"
            ]
            
            yield event.plain_result('\n'.join(status_msg))
            
        except Exception as e:
            self.logger.error(f"显示缓存状态失败: {e}")
            yield event.plain_result("获取缓存状态失败，请稍后重试！")
    
    # ========== 私有方法 ==========
    
    async def _get_user_display_name(self, event: AstrMessageEvent, group_id: str, user_id: str) -> str:
        """获取用户的群昵称，优先使用群昵称，其次使用QQ昵称"""
        try:
            # 检查是否为QQ群聊事件
            if not isinstance(event, AiocqhttpMessageEvent):
                # 非QQ群聊，使用原有的获取方式
                try:
                    nickname = event.get_sender_name()
                    return nickname or f"用户{user_id}"
                except Exception:
                    return f"用户{user_id}"
            
            # 检查群成员列表缓存
            cache_key = f"group_members_{group_id}"
            members_info = None
            
            if cache_key in self.group_members_cache:
                self.logger.debug(f"从缓存获取群 {group_id} 成员列表")
                members_info = self.group_members_cache[cache_key]
            else:
                # 缓存未命中，获取群成员列表
                self.logger.debug(f"获取群 {group_id} 成员列表并缓存")
                client = event.bot
                params = {"group_id": group_id}
                try:
                    members_info = await client.api.call_action('get_group_member_list', **params)
                    if members_info:
                        # 缓存群成员列表，设置合理的过期时间
                        self.group_members_cache[cache_key] = members_info
                        self.logger.debug(f"已缓存群 {group_id} 成员列表，共 {len(members_info)} 个成员")
                        
                        # 对于大群（成员数>500），记录警告
                        if len(members_info) > 500:
                            self.logger.warning(f"群 {group_id} 成员数较多（{len(members_info)}），建议调整缓存策略")
                except Exception as e:
                    self.logger.warning(f"获取群成员列表失败: {e}")
                    members_info = None
            
            if not members_info:
                # 如果无法获取群成员列表，回退到原有方式
                try:
                    nickname = event.get_sender_name()
                    return nickname or f"用户{user_id}"
                except Exception:
                    return f"用户{user_id}"
            
            # 优化大量群成员处理：使用字典查找替代列表遍历
            try:
                # 创建用户ID到成员信息的快速查找字典
                members_dict = {}
                for member in members_info:
                    member_user_id = str(member.get("user_id", ""))
                    if member_user_id:
                        members_dict[member_user_id] = member
                
                # 快速查找当前用户
                if user_id in members_dict:
                    member = members_dict[user_id]
                    # 优先使用群昵称(card)，其次使用QQ昵称(nickname)
                    display_name = member.get("card") or member.get("nickname")
                    if display_name:
                        return display_name
            except Exception as e:
                self.logger.warning(f"优化查找失败，回退到列表遍历: {e}")
                # 回退到原有的列表遍历方式
                for member in members_info:
                    if str(member.get("user_id", "")) == user_id:
                        display_name = member.get("card") or member.get("nickname")
                        if display_name:
                            return display_name
            
            # 如果在群成员列表中未找到，回退到原有方式
            try:
                nickname = event.get_sender_name()
                return nickname or f"用户{user_id}"
            except Exception:
                return f"用户{user_id}"
                
        except Exception as e:
            self.logger.error(f"获取用户群昵称失败: {e}")
            # 发生错误时回退到原有方式
            try:
                nickname = event.get_sender_name()
                return nickname or f"用户{user_id}"
            except Exception:
                return f"用户{user_id}"
    
    async def _get_group_name(self, event: AstrMessageEvent, group_id: str) -> str:
        """获取群名称，使用AstrBot官方API"""
        try:
            # 使用AstrBot官方API获取群聊数据（注意使用await）
            group_data = await event.get_group(group_id)
            
            if group_data:
                # 尝试从群数据中获取群名称
                group_name = None
                
                # 尝试不同的属性名
                if hasattr(group_data, 'group_name'):
                    group_name = group_data.group_name
                elif hasattr(group_data, 'name'):
                    group_name = group_data.name
                elif hasattr(group_data, 'title'):
                    group_name = group_data.title
                
                if group_name:
                    return group_name
            
            # 如果无法获取群名称，回退到默认格式
            return f"群{group_id}"
            
        except Exception as e:
            self.logger.error(f"获取群名称失败: {e}")
            # 发生错误时回退到默认格式
            return f"群{group_id}"
    
    async def _show_rank(self, event: AstrMessageEvent, rank_type: RankType):
        """显示排行榜"""
        try:
            # 获取群组ID和用户ID
            group_id = event.get_group_id()
            current_user_id = event.get_sender_id()
            
            if not group_id:
                yield event.plain_result("无法获取群组信息，请在群聊中使用此命令！")
                return
                
            if not current_user_id:
                yield event.plain_result("无法获取用户信息！")
                return
            
            group_id = str(group_id)
            current_user_id = str(current_user_id)
            
            # 获取群组数据
            group_data = await self.data_manager.get_group_data(group_id)
            
            if not group_data:
                yield event.plain_result("本群好像还没人说过话呢~")
                return
            
            # 根据类型筛选数据
            filtered_data = await self._filter_data_by_rank_type(group_data, rank_type)
            
            if not filtered_data:
                yield event.plain_result("这个时间段还没有人发言呢~")
                return
            
            # 对数据进行排序
            filtered_data = sorted(filtered_data, key=lambda x: x.total, reverse=True)
            
            # 获取配置
            config = await self.data_manager.get_config()
            
            # 生成标题
            title = self._generate_title(rank_type)
            
            # 创建群组信息
            group_info = GroupInfo(group_id=group_id)
            
            # 获取群名称
            group_name = await self._get_group_name(event, group_id)
            group_info.group_name = group_name
            
            # 根据配置选择显示模式
            if config.if_send_pic:
                try:
                    # 使用图片生成器
                    image_path = await self.image_generator.generate_rank_image(
                        filtered_data, group_info, title, current_user_id
                    )
                    
                    # 检查图片文件是否存在
                    import os
                    if os.path.exists(image_path):
                        # 发送图片
                        yield event.image_result(image_path)
                    else:
                        # 回退到文字模式
                        text_msg = self._generate_text_message(filtered_data, group_info, title, config)
                        yield event.plain_result(text_msg)
                        
                except Exception as e:
                    self.logger.error(f"生成图片失败: {e}")
                    # 回退到文字模式
                    text_msg = self._generate_text_message(filtered_data, group_info, title, config)
                    yield event.plain_result(text_msg)
            else:
                # 使用文字模式
                text_msg = self._generate_text_message(filtered_data, group_info, title, config)
                yield event.plain_result(text_msg)
        
        except Exception as e:
            self.logger.error(f"显示排行榜失败: {e}")
            yield event.plain_result("生成排行榜失败，请稍后重试")
    
    async def _filter_data_by_rank_type(self, group_data: List[UserData], rank_type: RankType) -> List[UserData]:
        """根据排行榜类型筛选数据并计算时间段内的发言次数"""
        current_date = datetime.now().date()
        
        if rank_type == RankType.TOTAL:
            return group_data
        
        elif rank_type == RankType.DAILY:
            # 计算今日发言次数
            filtered_users = []
            for user in group_data:
                if not user.history:
                    continue
                
                # 计算今日发言次数
                today_count = user.get_message_count_in_period(current_date, current_date)
                if today_count > 0:
                    # 创建新的UserData对象，仅包含今日发言数
                    new_user = UserData(
                        user_id=user.user_id,
                        nickname=user.nickname,
                        total=today_count,
                        history=[],  # 不需要历史记录
                        last_date=user.last_date
                    )
                    filtered_users.append(new_user)
            
            return filtered_users
        
        elif rank_type == RankType.WEEKLY:
            # 计算本周发言次数
            filtered_users = []
            
            # 获取本周开始日期（周一）
            days_since_monday = current_date.weekday()
            week_start = current_date - timedelta(days=days_since_monday)
            
            for user in group_data:
                if not user.history:
                    continue
                
                # 计算本周发言次数
                week_count = user.get_message_count_in_period(week_start, current_date)
                if week_count > 0:
                    # 创建新的UserData对象，仅包含本周发言数
                    new_user = UserData(
                        user_id=user.user_id,
                        nickname=user.nickname,
                        total=week_count,
                        history=[],  # 不需要历史记录
                        last_date=user.last_date
                    )
                    filtered_users.append(new_user)
            
            return filtered_users
        
        elif rank_type == RankType.MONTHLY:
            # 计算本月发言次数
            filtered_users = []
            
            # 获取本月开始日期
            month_start = current_date.replace(day=1)
            
            for user in group_data:
                if not user.history:
                    continue
                
                # 计算本月发言次数
                month_count = user.get_message_count_in_period(month_start, current_date)
                if month_count > 0:
                    # 创建新的UserData对象，仅包含本月发言数
                    new_user = UserData(
                        user_id=user.user_id,
                        nickname=user.nickname,
                        total=month_count,
                        history=[],  # 不需要历史记录
                        last_date=user.last_date
                    )
                    filtered_users.append(new_user)
            
            return filtered_users
        
        else:
            return group_data
    
    def _generate_title(self, rank_type: RankType) -> str:
        """生成标题"""
        now = datetime.now()
        
        if rank_type == RankType.TOTAL:
            return "总发言排行榜"
        elif rank_type == RankType.DAILY:
            return f"今日[{now.year}年{now.month}月{now.day}日]发言榜单"
        elif rank_type == RankType.WEEKLY:
            # 计算周数
            year_start = datetime(now.year, 1, 1)
            week_num = (now - year_start).days // 7 + 1
            return f"本周[{now.year}年{now.month}月第{week_num}周]发言榜单"
        elif rank_type == RankType.MONTHLY:
            return f"本月[{now.year}年{now.month}月]发言榜单"
        else:
            return "发言榜单"
    
    def _generate_text_message(self, users: List[UserData], group_info: GroupInfo, title: str, config: PluginConfig) -> str:
        """生成文字消息"""
        total_messages = sum(user.total for user in users)
        
        # 排序并限制数量
        sorted_users = sorted(users, key=lambda x: x.total, reverse=True)
        top_users = sorted_users[:config.rand]
        
        msg = [f"{title}\n发言总数: {total_messages}\n━━━━━━━━━━━━━━\n"]
        
        for i, user in enumerate(top_users):
            percentage = ((user.total / total_messages) * 100) if total_messages > 0 else 0
            msg.append(f"第{i + 1}名：{user.nickname}·{user.total}次（占比{percentage:.2f}%）\n")
        
        return ''.join(msg)