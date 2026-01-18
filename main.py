"""
AstrBot 群发言统计插件
统计群成员发言次数,生成排行榜
"""

# 标准库导入
import asyncio
import os
import re
import aiofiles
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Tuple

# 第三方库导入
from cachetools import TTLCache

# AstrBot框架导入
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.event.filter import EventMessageType
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger as astrbot_logger

# 本地模块导入
from .utils.data_manager import DataManager
from .utils.image_generator import ImageGenerator, ImageGenerationError
from .utils.validators import Validators, ValidationError

from .utils.models import (
    UserData, PluginConfig, GroupInfo, MessageDate, 
    RankType
)

# 异常处理装饰器导入
from .utils.exception_handlers import (
    exception_handler,
    data_operation_handler,
    file_operation_handler,
    safe_execute,
    log_exception,
    ExceptionConfig,
    safe_execute_with_context,
    safe_data_operation,
    safe_file_operation,
    safe_cache_operation,
    safe_config_operation,
    safe_calculation,
    safe_generation,
    safe_timer_operation
)

# ========== 全局常量定义 ==========
# 从集中管理的常量模块导入
from .utils.constants import (
    MAX_RANK_COUNT,
    USER_NICKNAME_CACHE_TTL,
    GROUP_MEMBERS_CACHE_TTL as CACHE_TTL_SECONDS
)

DEFAULT_KOOK_GUILD_ID = "8281529857959625"

@register("astrbot_plugin_message_stats", "xiaoruange39", "群发言统计插件", "1.7.0")
class MessageStatsPlugin(Star):
    """群发言统计插件
    
    该插件用于统计群组成员的发言次数,并生成多种类型的排行榜.
    支持自动监听群消息、手动记录、总榜/日榜/周榜/月榜/年榜等功能.
    
    主要功能:
        - 自动监听和记录群成员发言统计
        - 支持多种排行榜类型(总榜、日榜、周榜、月榜、年榜)
        - 提供图片和文字两种显示模式
        - 完整的配置管理系统
        - 权限控制和安全管理
        - 群成员昵称智能获取
        - 高效的缓存机制
        - 支持指令别名，方便用户使用
        
    排行榜指令别名:
        - 总榜: 发言榜 → 水群榜、B话榜、发言排行、排行榜、发言统计
        - 日榜: 今日发言榜 → 今日排行、日榜、今日发言排行、今日排行榜
        - 周榜: 本周发言榜 → 本周排行、周榜、本周发言排行、本周排行榜
        - 月榜: 本月发言榜 → 本月排行、月榜、本月发言排行、本月排行榜
        - 年榜: 本年发言榜 → 本年排行、年榜、本年发言排行、本年排行榜
        
    Attributes:
        data_manager (DataManager): 数据管理器,负责数据的存储和读取
        plugin_config (PluginConfig): 插件配置对象
        image_generator (ImageGenerator): 图片生成器,用于生成排行榜图片
        group_members_cache (TTLCache): 群成员列表缓存,5分钟TTL
        logger: 日志记录器
        initialized (bool): 插件初始化状态
        
    Example:
        >>> plugin = MessageStatsPlugin(context)
        >>> await plugin.initialize()
        >>> # 插件将自动开始监听群消息并记录统计
    """
    
    def __init__(self, context: Context, config: 'AstrBotConfig' = None):
        """初始化插件实例
        
        Args:
            context (Context): AstrBot上下文对象,包含插件运行环境信息
            config (AstrBotConfig): AstrBot配置的插件配置对象,通过Web界面设置
        """
        super().__init__(context)
        self.logger = astrbot_logger
        
        # 使用StarTools获取插件数据目录
        data_dir = StarTools.get_data_dir('message_stats')
        
        # 初始化组件
        self.data_manager = DataManager(data_dir)
        
        # 使用AstrBot的标准配置系统
        self.config = config
        self.plugin_config = self._convert_to_plugin_config()
        self.image_generator = None
        
        # 群组unified_msg_origin映射表 - 用于主动消息发送
        self.group_unified_msg_origins = {}
        
        # 群成员列表缓存 - 5分钟TTL,减少API调用
        self.group_members_cache = TTLCache(maxsize=100, ttl=CACHE_TTL_SECONDS)
        
        # 群成员字典缓存 - 用于快速查找群成员信息
        self.group_members_dict_cache = {}
        
        # 用户昵称缓存 - 缓存用户ID到昵称的映射，减少重复查找
        self.user_nickname_cache = TTLCache(maxsize=500, ttl=USER_NICKNAME_CACHE_TTL)
        
        # 定时任务管理器 - 延迟初始化
        self.timer_manager = None
    
    def _convert_to_plugin_config(self) -> PluginConfig:
        """将AstrBot配置转换为插件配置对象"""
        try:
            # 如果没有配置，使用默认配置
            if not self.config:
                self.logger.info("没有配置，使用默认配置")
                return PluginConfig()
            
            # 确保config是字典类型
            config_dict = dict(self.config) if hasattr(self.config, 'items') else {}
            
            # 使用PluginConfig.from_dict()方法进行安全的配置转换
            config = PluginConfig.from_dict(config_dict)
            
            # 记录配置转换情况
            if config.timer_enabled and config.timer_target_groups:
                self.logger.info(f"配置转换完成: 定时功能已启用, 目标群组: {config.timer_target_groups}")
                # 如果有unified_msg_origin信息，通知定时任务更新
                if hasattr(self, 'group_unified_msg_origins') and self.group_unified_msg_origins:
                    self.logger.info(f"当前unified_msg_origin映射表: {list(self.group_unified_msg_origins.keys())}")
            
            return config
        except Exception as e:
            self.logger.error(f"配置转换失败: {e}")
            self.logger.info("使用默认配置继续运行")
            return PluginConfig()
        
    async def _collect_group_unified_msg_origin(self, event: AstrMessageEvent):
        """收集群组的unified_msg_origin和群组名称
        
        Args:
            event: 消息事件对象
        """
        try:
            group_id = event.get_group_id()
            unified_msg_origin = event.unified_msg_origin
            
            if group_id and unified_msg_origin:
                group_id_str = str(group_id)
                
                # 检查是否是新的unified_msg_origin
                old_origin = self.group_unified_msg_origins.get(group_id_str)
                self.group_unified_msg_origins[group_id_str] = unified_msg_origin
                
                if old_origin != unified_msg_origin:
                    self.logger.info(f"已收集群组 {group_id} 的 unified_msg_origin")
                    
                    # 尝试获取并缓存群组名称
                    await self._cache_group_name(event, group_id_str)
                    
                    # 如果定时任务正在运行且需要此群组，更新配置
                    if self.timer_manager:
                        # 记录当前unified_msg_origin状态（安全截断）
                        origin_preview = unified_msg_origin[:20] + "..." if len(unified_msg_origin) > 20 else unified_msg_origin
                        self.logger.info(f"群组 {group_id} 的 unified_msg_origin: {origin_preview}")
                        
                        if self.plugin_config.timer_enabled and group_id_str in self.plugin_config.timer_target_groups:
                            self.logger.info(f"检测到目标群组 {group_id} 的 unified_msg_origin 已更新，更新定时任务配置...")
                            # 确保unified_msg_origin映射表是最新的
                            self.timer_manager.push_service.group_unified_msg_origins = self.group_unified_msg_origins
                            success = await self.timer_manager.update_config(self.plugin_config, self.group_unified_msg_origins)
                            if success:
                                self.logger.info(f"定时任务配置更新成功")
                            else:
                                self.logger.warning(f"定时任务配置更新失败")
                

        except (AttributeError, KeyError, TypeError) as e:
            self.logger.error(f"收集群组unified_msg_origin失败: {e}")
        except (RuntimeError, OSError, IOError, ImportError, ValueError) as e:
            self.logger.error(f"收集群组unified_msg_origin失败(系统错误): {e}")

    def _get_platform_name(self, event: AstrMessageEvent) -> Optional[str]:
        platform_meta = getattr(event, 'platform_meta', None)
        return getattr(platform_meta, 'name', None)

    def _extract_kook_guild_id(self, event: AstrMessageEvent) -> Optional[str]:
        message_obj = getattr(event, 'message_obj', None)
        raw = getattr(message_obj, 'raw_message', None) if message_obj else None
        if raw is None:
            raw = getattr(event, 'raw_message', None)
        if not isinstance(raw, dict):
            return None
        # 兼容部分适配器将 payload 包在 d 字段中
        payload = raw.get('d') if isinstance(raw.get('d'), dict) else raw
        extra = payload.get('extra') or {}
        return (
            payload.get('guild_id')
            or extra.get('guild_id')
            or (extra.get('guild') or {}).get('id')
        )

    def _extract_kook_roles(self, event: AstrMessageEvent) -> List[int]:
        """从 KOOK 原始消息中提取 author.roles（角色ID列表）。"""
        message_obj = getattr(event, 'message_obj', None)
        raw = getattr(message_obj, 'raw_message', None) if message_obj else None
        if raw is None:
            raw = getattr(event, 'raw_message', None)
        if not isinstance(raw, dict):
            return []

        payload = raw.get('d') if isinstance(raw.get('d'), dict) else raw
        extra = payload.get('extra') or {}
        author = extra.get('author') or {}
        roles = author.get('roles') or []

        if isinstance(roles, (int, str)):
            roles = [roles]
        if not isinstance(roles, list):
            return []

        role_ids: List[int] = []
        for r in roles:
            try:
                role_ids.append(int(r))
            except (TypeError, ValueError):
                continue

        return sorted(set(role_ids))

    def _resolve_stats_group_id(self, event: AstrMessageEvent) -> Optional[str]:
        platform_name = self._get_platform_name(event)
        group_id = event.get_group_id()
        if platform_name == "kook":
            guild_id = self._extract_kook_guild_id(event)
            if not guild_id and self.plugin_config and self.plugin_config.detailed_logging_enabled:
                self.logger.debug("KOOK 未提供 guild_id，回退使用 channel_id 统计")
            return str(guild_id) if guild_id else (str(group_id) if group_id else None)
        return str(group_id) if group_id else None
    async def _cache_group_name(self, event: AstrMessageEvent, group_id: str):
        """获取并缓存群组名称
        
        从事件或API获取群组名称，更新到 timer_manager 的缓存和数据文件中。
        
        Args:
            event: 消息事件对象
            group_id: 群组ID
        """
        try:
            group_name = None
            
            # 方法1: 尝试通过 bot API 获取群组信息
            if hasattr(event, 'bot') and event.bot:
                try:
                    if hasattr(event.bot, 'api'):
                        group_info = await event.bot.api.call_action(
                            'get_group_info', 
                            group_id=int(group_id)
                        )
                        if group_info and isinstance(group_info, dict):
                            group_name = group_info.get('group_name')
                except (AttributeError, TypeError, ValueError, asyncio.TimeoutError) as e:
                    self.logger.debug(f"通过API获取群名失败: {e}")
            
            # 如果获取到群名，更新缓存
            if group_name:
                self.logger.info(f"已获取群组 {group_id} 的名称: {group_name}")
                
                # 更新到 timer_manager 的内存缓存
                if self.timer_manager:
                    self.timer_manager.update_group_name_cache(group_id, group_name)
                
                # 更新到数据文件（下次保存时会自动包含）
                # 这里我们可以单独保存群名，但为了简化，我们只更新内存缓存
                # 群名会在下次保存群组数据时一并保存
                
        except (AttributeError, KeyError, TypeError, RuntimeError) as e:
            self.logger.debug(f"缓存群组名称失败: {e}")
    
    async def _collect_group_unified_msg_origins(self):
        """收集所有群组的unified_msg_origin（从缓存中获取）"""
        # 这个方法用于初始化时的批量收集
        # 由于没有event对象，我们先返回空字典
        # 实际的收集将在命令执行时进行
        return self.group_unified_msg_origins.copy()
    
    # ========== 类常量定义 ==========
    
    # 排行榜数量限制常量（使用模块级常量）
    RANK_COUNT_MIN = 1
    # MAX_RANK_COUNT 已从 constants 模块导入，不再重复定义
    
    # 图片模式别名常量
    IMAGE_MODE_ENABLE_ALIASES = {'1', 'true', '开', 'on', 'yes'}
    IMAGE_MODE_DISABLE_ALIASES = {'0', 'false', '关', 'off', 'no'}
    
    async def initialize(self):
        """初始化插件
        
        异步初始化插件的所有组件,包括数据管理器、配置和图片生成器.
        
        Raises:
            OSError: 当数据目录创建失败时抛出
            IOError: 当配置文件读写失败时抛出
            Exception: 其他初始化相关的异常
            
        Returns:
            None: 无返回值,初始化成功后设置initialized状态
            
        Example:
            >>> plugin = MessageStatsPlugin(context)
            >>> await plugin.initialize()
            >>> print(plugin.initialized)
            True
        """
        try:
            self.logger.info("群发言统计插件初始化中...")
            
            # 步骤1: 初始化数据管理器
            await self._initialize_data_manager()
            
            # 步骤2: 加载插件配置和创建图片生成器
            await self._load_plugin_config()
            
            # 步骤3: 设置数据管理器的配置引用
            self.data_manager.set_plugin_config(self.plugin_config)
            
            # 步骤4: 初始化定时任务管理器
            await self._initialize_timer_manager()
            
            # 步骤5: 设置缓存和最终初始化状态
            await self._setup_caches()
            
            self.logger.info("群发言统计插件初始化完成")
            
        except (OSError, IOError) as e:
            self.logger.error(f"插件初始化失败: {e}")
            raise
    
    async def _initialize_data_manager(self):
        """初始化数据管理器
        
        负责初始化数据管理器的核心功能，包括目录创建和基础设置。
        
        Raises:
            OSError: 当数据目录创建失败时抛出
            IOError: 当文件操作失败时抛出
            
        Returns:
            None: 无返回值
        """
        await self.data_manager.initialize()
    
    async def _load_plugin_config(self):
        """更新插件配置和创建图片生成器
        
        从AstrBot配置更新插件配置，并创建和初始化图片生成器。
        
        Raises:
            ImportError: 当导入图片生成器相关模块失败时抛出
            
        Returns:
            None: 无返回值
        """
        # 更新插件配置（从AstrBot配置转换）
        self.plugin_config = self._convert_to_plugin_config()
        
        # 创建图片生成器
        self.image_generator = ImageGenerator(self.plugin_config)
        
        # 初始化图片生成器
        try:
            await self.image_generator.initialize()
            self.logger.info("图片生成器初始化成功")
        except ImageGenerationError as e:
            self.logger.warning(f"图片生成器初始化失败: {e}")
        
        # 记录当前配置状态
        self.logger.info(f"当前配置: 图片模式={self.plugin_config.if_send_pic}, 显示人数={self.plugin_config.rand}")
    
    async def _initialize_timer_manager(self):
        """初始化定时任务管理器
        
        创建并初始化定时任务管理器，尝试启动定时任务（不阻塞初始化过程）。
        
        Raises:
            ImportError: 当导入定时任务管理器模块失败时抛出
            OSError: 当系统操作失败时抛出
            IOError: 当文件操作失败时抛出
            RuntimeError: 当运行时错误发生时抛出
            AttributeError: 当属性访问错误时抛出
            ValueError: 当参数值错误时抛出
            TypeError: 当类型错误时抛出
            ConnectionError: 当连接错误时抛出
            asyncio.TimeoutError: 当异步操作超时时抛出
            
        Returns:
            None: 无返回值
        """
        try:
            from .utils.timer_manager import TimerManager
            self.timer_manager = TimerManager(self.data_manager, self.image_generator, self.context, self.group_unified_msg_origins)
            self.logger.info("定时任务管理器初始化成功")
            # 注意：定时任务的启动在 _setup_caches 中统一进行，避免重复启动
                    
        except (ImportError, OSError, IOError) as e:
            self.logger.warning(f"定时任务管理器初始化失败: {e}")
            self.timer_manager = None
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            self.logger.warning(f"定时任务管理器初始化失败(运行时错误): {e}")
            self.timer_manager = None
    
    async def _setup_caches(self):
        """设置缓存和最终初始化状态
        
        完成插件初始化后的最终设置，包括缓存配置和状态标记。
        
        Raises:
            无特定异常抛出
            
        Returns:
            None: 无返回值
        """
        self.initialized = True
        
        # 插件初始化完成后，尝试启动定时任务
        if self.timer_manager and self.plugin_config.timer_enabled:
            try:
                self.logger.info("插件初始化完成，尝试启动定时任务...")
                # 确保unified_msg_origin映射表被正确传递
                if hasattr(self.timer_manager, 'push_service'):
                    self.timer_manager.push_service.group_unified_msg_origins = self.group_unified_msg_origins
                    self.logger.info(f"定时任务管理器已更新unified_msg_origin映射表: {list(self.group_unified_msg_origins.keys())}")
                else:
                    self.logger.warning("定时任务管理器未完全初始化，无法更新unified_msg_origin映射表")
                
                success = await self.timer_manager.update_config(self.plugin_config, self.group_unified_msg_origins)
                if success:
                    self.logger.info("定时任务启动成功")
                else:
                    self.logger.warning("定时任务启动失败，可能是因为群组unified_msg_origin尚未收集")
                    if self.plugin_config.timer_target_groups:
                        missing_groups = [g for g in self.plugin_config.timer_target_groups if g not in self.group_unified_msg_origins]
                        if missing_groups:
                            self.logger.info(f"缺少unified_msg_origin的群组: {missing_groups}")
                            self.logger.info("💡 提示: 在这些群组中发送任意消息以收集unified_msg_origin")
            except (ImportError, AttributeError, RuntimeError) as e:
                self.logger.warning(f"定时任务启动失败: {e}")
                # 不影响插件的正常使用
            except (ValueError, TypeError, ConnectionError, asyncio.TimeoutError, KeyError) as e:
                # 修复：替换过于宽泛的Exception为具体异常类型
                self.logger.warning(f"定时任务启动失败(参数错误): {e}")
                # 不影响插件的正常使用
    
    async def terminate(self):
        """插件卸载清理
        
        异步清理插件的所有资源,包括浏览器实例、缓存和临时文件.
        确保插件卸载时不会留下资源泄漏.
        
        Raises:
            OSError: 当清理文件或目录失败时抛出
            IOError: 当文件操作失败时抛出
            Exception: 其他清理相关的异常
            
        Returns:
            None: 无返回值,清理完成后设置initialized状态为False
            
        Example:
            >>> await plugin.terminate()
            >>> print(plugin.initialized)
            False
        """
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
            
        except (OSError, IOError) as e:
            self.logger.error(f"插件卸载失败: {e}")
    
    # ========== 消息监听 ==========
    
    @filter.event_message_type(EventMessageType.ALL)
    async def auto_message_listener(self, event: AstrMessageEvent):
        """自动消息监听器 - 监听所有消息并记录群成员发言统计"""
        # 跳过命令消息
        message_str = getattr(event, 'message_str', '')
        if message_str.startswith(('%', '/')):
            return
        
        # 获取基本信息
        group_id = self._resolve_stats_group_id(event)
        user_id = event.get_sender_id()
        
        # 跳过非群聊或无效用户
        if not group_id or not user_id:
            return
        
        # 转换为字符串并跳过机器人
        group_id, user_id = str(group_id), str(user_id)
        if self._is_bot_message(event, user_id):
            return
        
        # 检查群聊是否在屏蔽列表中
        if self._is_blocked_group(group_id):
            if self.plugin_config.detailed_logging_enabled:
                self.logger.debug(f"群聊 {group_id} 在屏蔽列表中，跳过统计")
            return
        
        # 收集群组的unified_msg_origin（重要：用于定时推送）
        await self._collect_group_unified_msg_origin(event)
        
        # 获取用户昵称并记录统计
        nickname_group_id = event.get_group_id() or group_id
        nickname = await self._get_user_display_name(event, nickname_group_id, user_id)
        roles = self._extract_kook_roles(event) if self._get_platform_name(event) == "kook" else None
        await self._record_message_stats(group_id, user_id, nickname, roles=roles)
    
    def _is_bot_message(self, event: AstrMessageEvent, user_id: str) -> bool:
        """检查是否为机器人消息"""
        try:
            platform_name = self._get_platform_name(event)
            if platform_name == "kook":
                message_obj = getattr(event, 'message_obj', None)
                raw = getattr(message_obj, 'raw_message', None) if message_obj else None
                if raw is None:
                    raw = getattr(event, 'raw_message', None)
                if isinstance(raw, dict):
                    bot_flag = ((raw.get('extra') or {}).get('author') or {}).get('bot')
                    if bot_flag is True:
                        return True
                return False

            self_id = event.get_self_id()
            return self_id and user_id == str(self_id)
        except (AttributeError, KeyError, TypeError):
            return False
    
    async def _record_message_stats(
        self,
        group_id: str,
        user_id: str,
        nickname: str,
        roles: Optional[List[int]] = None,
    ):
        """记录消息统计
        
        内部方法,用于记录群成员的消息统计数据.会自动验证输入参数并更新数据.
        
        Args:
            group_id (str): 群组ID,必须是5-12位数字字符串
            user_id (str): 用户ID,必须是1-20位数字字符串
            nickname (str): 用户昵称,会进行HTML转义和安全验证
            
        Raises:
            ValueError: 当参数验证失败时抛出
            TypeError: 当参数类型错误时抛出
            KeyError: 当数据格式错误时抛出
            
        Returns:
            None: 无返回值,记录结果通过日志输出
            
        Example:
            >>> await self._record_message_stats("123456789", "987654321", "用户昵称")
            # 将在数据管理器中更新该用户的发言统计
        """
        try:
            # 步骤0: 检查是否为屏蔽用户
            if self._is_blocked_user(user_id):
                if self.plugin_config.detailed_logging_enabled:
                    self.logger.debug(f"用户 {user_id} 在屏蔽列表中，跳过统计")
                return
            
            # 步骤1: 安全处理昵称，确保不为空
            if not nickname or not nickname.strip():
                nickname = f"用户{user_id}"
                self.logger.warning(f"昵称获取失败，使用默认昵称: {nickname}")
            
            # 步骤2: 验证输入数据
            validated_data = await self._validate_message_data(group_id, user_id, nickname)
            group_id, user_id, nickname = validated_data
            
            # 步骤3: 处理消息统计和记录
            await self._process_message_stats(group_id, user_id, nickname, roles=roles)
            
        except ValueError as e:
            self.logger.error(f"记录消息统计失败(参数验证错误): {e}", exc_info=True)
        except TypeError as e:
            self.logger.error(f"记录消息统计失败(类型错误): {e}", exc_info=True)
        except KeyError as e:
            self.logger.error(f"记录消息统计失败(数据格式错误): {e}", exc_info=True)
        except asyncio.TimeoutError as e:
            self.logger.error(f"记录消息统计失败(超时错误): {e}", exc_info=True)
        except ConnectionError as e:
            self.logger.error(f"记录消息统计失败(连接错误): {e}", exc_info=True)
        except asyncio.CancelledError as e:
            self.logger.error(f"记录消息统计失败(操作取消): {e}", exc_info=True)
        except (IOError, OSError) as e:
            self.logger.error(f"记录消息统计失败(系统错误): {e}", exc_info=True)
        except AttributeError as e:
            self.logger.error(f"记录消息统计失败(属性错误): {e}", exc_info=True)
        except RuntimeError as e:
            self.logger.error(f"记录消息统计失败(运行时错误): {e}", exc_info=True)
        except ImportError as e:
            self.logger.error(f"记录消息统计失败(导入错误): {e}", exc_info=True)
        except (FileNotFoundError, PermissionError, UnicodeError, MemoryError, SystemError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"记录消息统计失败(系统资源错误): {e}", exc_info=True)
    
    @data_operation_handler('validate', '消息数据参数')
    async def _validate_message_data(self, group_id: str, user_id: str, nickname: str) -> tuple:
        """验证消息数据参数
        
        验证输入的群组ID、用户ID和昵称参数，确保数据格式正确。
        
        Args:
            group_id (str): 群组ID
            user_id (str): 用户ID
            nickname (str): 用户昵称
            
        Returns:
            tuple: 验证后的 (group_id, user_id, nickname) 元组
            
        Raises:
            ValueError: 当参数验证失败时抛出
            TypeError: 当参数类型错误时抛出
        """
        # 验证数据
        group_id = Validators.validate_group_id(group_id)
        user_id = Validators.validate_user_id(user_id)
        nickname = Validators.validate_nickname(nickname)
        
        return group_id, user_id, nickname
    
    async def _process_message_stats(
        self,
        group_id: str,
        user_id: str,
        nickname: str,
        roles: Optional[List[int]] = None,
    ) -> bool:
        """处理消息统计和记录
        
        执行实际的消息统计更新操作，并记录结果日志。
        智能缓存管理：检查昵称变化，只在必要时更新缓存。
        
        Args:
            group_id (str): 验证后的群组ID
            user_id (str): 验证后的用户ID
            nickname (str): 验证后的用户昵称
            
        Raises:
            KeyError: 当数据格式错误时抛出
            asyncio.TimeoutError: 当异步操作超时时抛出
            ConnectionError: 当连接错误时抛出
            asyncio.CancelledError: 当操作取消时抛出
            IOError: 当文件操作错误时抛出
            OSError: 当系统操作错误时抛出
            AttributeError: 当属性访问错误时抛出
            RuntimeError: 当运行时错误时抛出
            ImportError: 当导入错误时抛出
            FileNotFoundError: 当文件未找到时抛出
            PermissionError: 当权限错误时抛出
            UnicodeError: 当编码错误时抛出
            MemoryError: 当内存错误时抛出
            SystemError: 当系统错误时抛出
        """
        # 直接使用data_manager更新用户消息
        success = await self.data_manager.update_user_message(group_id, user_id, nickname, roles=roles)
        
        if success:
            # 智能缓存管理：检查昵称变化
            nickname_cache_key = f"nickname_{user_id}"
            cached_nickname = self.user_nickname_cache.get(nickname_cache_key)
            
            # 只在昵称变化时才更新缓存（节省API调用）
            if cached_nickname != nickname:
                self.user_nickname_cache[nickname_cache_key] = nickname
                
                if self.plugin_config.detailed_logging_enabled:
                    self.logger.debug(f"昵称发生变化，更新缓存: {cached_nickname} -> {nickname}")
                    self.logger.debug(f"记录消息统计: {nickname}")
            else:
                # 昵称未变化，只记录基本日志
                if self.plugin_config.detailed_logging_enabled:
                    self.logger.debug(f"昵称未变化，保持缓存: {nickname}")
                    self.logger.debug(f"记录消息统计: {nickname}")
        else:
            self.logger.error(f"记录消息统计失败: {nickname}")
        return success
    
    # ========== 排行榜命令 ==========
    

    
    @filter.command("发言榜", alias={'水群榜', 'B话榜', '发言排行', '发言统计'})
    async def show_full_rank(self, event: AstrMessageEvent):
        """显示总排行榜，别名：水群榜/B话榜/发言排行/发言统计"""
        async for result in self._show_rank(event, RankType.TOTAL):
            yield result
    
    @filter.command("今日发言榜", alias={'今日水群榜', '今日发言排行', '今日B话榜'})
    async def show_daily_rank(self, event: AstrMessageEvent):
        """显示今日排行榜，别名：今日水群榜/今日发言排行/今日B话榜"""
        async for result in self._show_rank(event, RankType.DAILY):
            yield result
    
    @filter.command("本周发言榜", alias={'本周水群榜', '本周发言排行', '本周B话榜'})
    async def show_weekly_rank(self, event: AstrMessageEvent):
        """显示本周排行榜，别名：本周水群榜/本周发言排行/本周B话榜"""
        async for result in self._show_rank(event, RankType.WEEKLY):
            yield result
    
    @filter.command("本月发言榜", alias={'本月水群榜', '本月发言排行', '本月B话榜'})
    async def show_monthly_rank(self, event: AstrMessageEvent):
        """显示本月排行榜，别名：本月水群榜/本月发言排行/本月B话榜"""
        async for result in self._show_rank(event, RankType.MONTHLY):
            yield result
    
    @filter.command("本年发言榜", alias={'本年水群榜', '本年发言排行', '本年B话榜', '年榜'})
    async def show_yearly_rank(self, event: AstrMessageEvent):
        """显示本年排行榜，别名：本年水群榜/本年发言排行/本年B话榜/年榜"""
        async for result in self._show_rank(event, RankType.YEARLY):
            yield result
    
    @filter.command("去年发言榜", alias={'去年水群榜', '去年发言排行', '去年B话榜'})
    async def show_last_year_rank(self, event: AstrMessageEvent):
        """显示去年排行榜，别名：去年水群榜/去年发言排行/去年B话榜"""
        async for result in self._show_rank(event, RankType.LAST_YEAR):
            yield result
    
    # ========== 设置命令 ==========
    
    @filter.command("设置发言榜数量")
    async def set_rank_count(self, event: AstrMessageEvent):
        """设置排行榜显示人数"""
        try:
            # 获取群组ID
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息,请在群聊中使用此命令！")
                return
            
            group_id = str(group_id)
            
            # 获取参数
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            if not args:
                yield event.plain_result("请指定数量！用法:#设置发言榜数量 10")
                return
            
            # 验证数量
            try:
                count = int(args[0])
                if count < self.RANK_COUNT_MIN or count > self.MAX_RANK_COUNT:
                    yield event.plain_result(f"数量必须在{self.RANK_COUNT_MIN}-{self.MAX_RANK_COUNT}之间！")
                    return
            except ValueError:
                yield event.plain_result("数量必须是数字！")
                return
            
            # 保存配置
            config = await self.data_manager.get_config()
            config.rand = count
            await self.data_manager.save_config(config)
            
            yield event.plain_result(f"排行榜显示人数已设置为 {count} 人！")
            
        except ValueError as e:
            self.logger.error(f"设置排行榜数量失败(参数错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except TypeError as e:
            self.logger.error(f"设置排行榜数量失败(类型错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except KeyError as e:
            self.logger.error(f"设置排行榜数量失败(数据格式错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except (IOError, OSError, FileNotFoundError) as e:
            self.logger.error(f"设置排行榜数量失败(文件操作错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except AttributeError as e:
            self.logger.error(f"设置排行榜数量失败(属性错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except RuntimeError as e:
            self.logger.error(f"设置排行榜数量失败(运行时错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except (ConnectionError, asyncio.TimeoutError, ImportError, PermissionError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"设置排行榜数量失败(网络或系统错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")

    @filter.command("设置发言榜图片")
    async def set_image_mode(self, event: AstrMessageEvent):
        """设置排行榜的显示模式（图片或文字）
        
        根据用户输入的参数设置排行榜的显示模式：
        - 1/true/开/on/yes: 设置为图片模式
        - 0/false/关/off/no: 设置为文字模式
        
        返回相应的设置成功提示信息。
        """
        try:
            # 获取群组ID
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息,请在群聊中使用此命令！")
                return
            
            group_id = str(group_id)
            
            # 获取参数
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            if not args:
                yield event.plain_result("请指定模式！用法:#设置发言榜图片 1")
                return
            
            # 验证模式
            mode = args[0].lower()
            if mode in self.IMAGE_MODE_ENABLE_ALIASES:
                send_pic = 1
                mode_text = "图片模式"
            elif mode in self.IMAGE_MODE_DISABLE_ALIASES:
                send_pic = 0
                mode_text = "文字模式"
            else:
                yield event.plain_result("模式参数错误！可用:1/true/开 或 0/false/关")
                return
            
            # 保存配置
            config = await self.data_manager.get_config()
            config.if_send_pic = send_pic
            await self.data_manager.save_config(config)
            
            yield event.plain_result(f"排行榜显示模式已设置为 {mode_text}！")
            
        except ValueError as e:
            self.logger.error(f"设置图片模式失败(参数错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except TypeError as e:
            self.logger.error(f"设置图片模式失败(类型错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except KeyError as e:
            self.logger.error(f"设置图片模式失败(数据格式错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except (IOError, OSError, FileNotFoundError) as e:
            self.logger.error(f"设置图片模式失败(文件操作错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except AttributeError as e:
            self.logger.error(f"设置图片模式失败(属性错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except RuntimeError as e:
            self.logger.error(f"设置图片模式失败(运行时错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
        except (ConnectionError, asyncio.TimeoutError, ImportError, PermissionError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"设置图片模式失败(网络或系统错误): {e}", exc_info=True)
            yield event.plain_result("设置失败,请稍后重试")
    
    @filter.command("清除发言榜单")
    async def clear_message_ranking(self, event: AstrMessageEvent):
        """清除发言榜单"""
        try:
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息,请在群聊中使用此命令！")
                return
            group_id = str(group_id)
            
            success = await self.data_manager.clear_group_data(group_id)
            
            if success:
                yield event.plain_result("本群发言榜单已清除！")
            else:
                yield event.plain_result("清除榜单失败,请稍后重试！")
            
        except (IOError, OSError, FileNotFoundError) as e:
            self.logger.error(f"清除榜单失败: {e}")
            yield event.plain_result("清除榜单失败,请稍后重试！")
    
    @filter.command("刷新发言榜群成员缓存")
    async def refresh_group_members_cache(self, event: AstrMessageEvent):
        """刷新群成员列表缓存"""
        try:
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取群组信息,请在群聊中使用此命令！")
                return
            group_id = str(group_id)
            
            # 清除特定群的成员缓存
            cache_key = f"group_members_{group_id}"
            if cache_key in self.group_members_cache:
                del self.group_members_cache[cache_key]
                self.logger.info(f"刷新群 {group_id} 成员缓存")
            else:
                self.logger.info(f"群 {group_id} 没有需要刷新的成员缓存")
            
            # 清除群成员字典缓存（重要！用于昵称获取）
            dict_cache_key = f"group_members_dict_{group_id}"
            if dict_cache_key in self.group_members_dict_cache:
                del self.group_members_dict_cache[dict_cache_key]
                self.logger.info(f"刷新群 {group_id} 字典缓存")
            
            # 同时清除昵称缓存（快速修复昵称更新问题）
            self.clear_user_cache()  # 清除所有用户昵称缓存
            self.logger.info(f"刷新群 {group_id} 昵称缓存")
            
            # 为现有用户更新最新昵称
            try:
                group_data = await self.data_manager.get_group_data(group_id)
                if group_data:
                    # 获取群成员最新信息
                    members_info = await self._fetch_group_members_from_api(event, group_id)
                    if members_info:
                        # 构建用户ID到最新昵称的映射
                        member_nickname_map = {}
                        for member in members_info:
                            user_id = str(member.get("user_id", ""))
                            if user_id:
                                # 使用群的昵称获取逻辑
                                display_name = self._get_display_name_from_member(member)
                                if display_name:
                                    member_nickname_map[user_id] = display_name
                        
                        # 更新用户数据中的昵称
                        updated_count = 0
                        for user in group_data:
                            if user.user_id in member_nickname_map:
                                old_nickname = user.nickname
                                new_nickname = member_nickname_map[user.user_id]
                                if old_nickname != new_nickname:
                                    user.nickname = new_nickname
                                    updated_count += 1
                                    self.logger.info(f"更新用户 {user.user_id} 昵称: {old_nickname} -> {new_nickname}")
                        
                        # 保存更新后的数据
                        if updated_count > 0:
                            await self.data_manager.save_group_data(group_id, group_data)
                            self.logger.info(f"群 {group_id} 共有 {updated_count} 个用户的昵称已更新")
            except (AttributeError, KeyError, TypeError, ValueError, RuntimeError, IOError, OSError) as e:
                self.logger.error(f"更新用户昵称失败: {e}", exc_info=True)
            
            yield event.plain_result("群成员缓存、字典缓存和昵称缓存已全部刷新！")
            
        except AttributeError as e:
            self.logger.error(f"刷新群成员缓存失败(属性错误): {e}", exc_info=True)
            yield event.plain_result("刷新缓存失败,请稍后重试！")
        except KeyError as e:
            self.logger.error(f"刷新群成员缓存失败(数据格式错误): {e}", exc_info=True)
            yield event.plain_result("刷新缓存失败,请稍后重试！")
        except TypeError as e:
            self.logger.error(f"刷新群成员缓存失败(类型错误): {e}", exc_info=True)
            yield event.plain_result("刷新缓存失败,请稍后重试！")
        except (IOError, OSError) as e:
            self.logger.error(f"刷新群成员缓存失败(系统错误): {e}", exc_info=True)
            yield event.plain_result("刷新缓存失败,请稍后重试！")
        except RuntimeError as e:
            self.logger.error(f"刷新群成员缓存失败(运行时错误): {e}", exc_info=True)
            yield event.plain_result("刷新缓存失败,请稍后重试！")
        except (ConnectionError, asyncio.TimeoutError, ImportError, PermissionError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"刷新群成员缓存失败(网络或系统错误): {e}", exc_info=True)
            yield event.plain_result("刷新缓存失败,请稍后重试！")
    
    @filter.command("发言榜缓存状态")
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
            
        except ValueError as e:
            self.logger.error(f"显示缓存状态失败(参数错误): {e}", exc_info=True)
            yield event.plain_result("获取缓存状态失败,请稍后重试！")
        except TypeError as e:
            self.logger.error(f"显示缓存状态失败(类型错误): {e}", exc_info=True)
            yield event.plain_result("获取缓存状态失败,请稍后重试！")
        except KeyError as e:
            self.logger.error(f"显示缓存状态失败(数据格式错误): {e}", exc_info=True)
            yield event.plain_result("获取缓存状态失败,请稍后重试！")
        except (IOError, OSError) as e:
            self.logger.error(f"显示缓存状态失败(系统错误): {e}", exc_info=True)
            yield event.plain_result("获取缓存状态失败,请稍后重试！")
        except AttributeError as e:
            self.logger.error(f"显示缓存状态失败(属性错误): {e}", exc_info=True)
            yield event.plain_result("获取缓存状态失败,请稍后重试！")
        except RuntimeError as e:
            self.logger.error(f"显示缓存状态失败(运行时错误): {e}", exc_info=True)
            yield event.plain_result("获取缓存状态失败,请稍后重试！")
        except (ConnectionError, asyncio.TimeoutError, ImportError, PermissionError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"显示缓存状态失败(网络或系统错误): {e}", exc_info=True)
            yield event.plain_result("获取缓存状态失败,请稍后重试！")
    
    # ========== 私有方法 ==========
    
    async def _get_user_display_name(self, event: AstrMessageEvent, group_id: str, user_id: str) -> str:
        """获取用户的群昵称,优先使用群昵称,其次使用QQ昵称（重构版 - 跨平台兼容）"""
        # 优先使用统一的昵称获取逻辑
        nickname = await self._get_user_nickname_unified(event, group_id, user_id)
        
        # 如果统一逻辑失败，使用备用方案
        if nickname == f"用户{user_id}":
            return await self._get_fallback_nickname(event, user_id)
        
        return nickname
    
    @data_operation_handler('extract', '群成员昵称数据')
    def _get_display_name_from_member(self, member: Dict[str, Any]) -> Optional[str]:
        """从群成员信息中提取显示昵称
        
        提取用户昵称的辅助函数，避免重复的逻辑
        
        Args:
            member (Dict[str, Any]): 群成员信息字典
            
        Returns:
            Optional[str]: 用户的显示昵称，如果获取失败则返回None
        """
        return member.get("card") or member.get("nickname")

    async def _get_user_nickname_unified(self, event: AstrMessageEvent, group_id: str, user_id: str) -> str:
        """统一的用户昵称获取方法 - 性能优先版本（缓存优先策略）
        
        策略：采用分层缓存策略，性能最优
        1. 从昵称缓存获取（最高效且常用）
        2. 从群成员字典缓存获取（中等效率）
        3. 从API获取（仅在前两级缓存失效时，确保准确性）
        4. 返回默认昵称
        
        Args:
            event (AstrMessageEvent): 消息事件对象
            group_id (str): 群组ID
            user_id (str): 用户ID
            
        Returns:
            str: 用户的显示昵称，如果都失败则返回 "用户{user_id}"
        """
        # 步骤1: 从昵称缓存获取（最高效优先）
        nickname = await self._get_from_nickname_cache(user_id)
        if nickname:
            return nickname
        
        # 步骤2: 从群成员字典缓存获取（中等效率）
        nickname = await self._get_from_dict_cache(group_id, user_id)
        if nickname:
            return nickname
        
        # 步骤3: 从API获取（仅在前两级缓存失效时调用，确保准确性）
        nickname = await self._fetch_and_cache_from_api(event, group_id, user_id)
        if nickname:
            return nickname
        
        # 步骤4: 返回默认昵称
        return f"用户{user_id}"
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    async def _get_from_nickname_cache(self, user_id: str) -> Optional[str]:
        """从昵称缓存获取昵称"""
        nickname_cache_key = f"nickname_{user_id}"
        cached_nickname = self.user_nickname_cache.get(nickname_cache_key)
        
        # 如果缓存存在但TTL即将过期，标记为需要刷新（由上层逻辑处理）
        if cached_nickname:
            # TTL即将过期的用户可以在下次使用时自动刷新
            return cached_nickname
        
        # 缓存miss，返回None触发上层逻辑从其他源获取
        return cached_nickname
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    async def _get_from_dict_cache(self, group_id: str, user_id: str) -> Optional[str]:
        """从群成员字典缓存获取昵称"""
        dict_cache_key = f"group_members_dict_{group_id}"
        if dict_cache_key in self.group_members_dict_cache:
            members_dict = self.group_members_dict_cache[dict_cache_key]
            if user_id in members_dict:
                member = members_dict[user_id]
                display_name = self._get_display_name_from_member(member)
                if display_name:
                    # 缓存到昵称缓存
                    nickname_cache_key = f"nickname_{user_id}"
                    self.user_nickname_cache[nickname_cache_key] = display_name
                    return display_name
        return None
    
    async def _fetch_and_cache_from_api(self, event: AstrMessageEvent, group_id: str, user_id: str) -> Optional[str]:
        """从API获取群成员信息并缓存"""
        try:
            members_info = await self._fetch_group_members_from_api(event, group_id)
            if members_info:
                # 重建字典缓存
                dict_cache_key = f"group_members_dict_{group_id}"
                members_dict = {str(m.get("user_id", "")): m for m in members_info if m.get("user_id")}
                self.group_members_dict_cache[dict_cache_key] = members_dict
                
                # 查找用户
                if user_id in members_dict:
                    member = members_dict[user_id]
                    display_name = self._get_display_name_from_member(member)
                    if display_name:
                        # 缓存到昵称缓存
                        nickname_cache_key = f"nickname_{user_id}"
                        self.user_nickname_cache[nickname_cache_key] = display_name
                        return display_name
        except (AttributeError, KeyError, TypeError) as e:
            self.logger.warning(f"获取群成员信息失败(数据格式错误): {e}")
        except (ConnectionError, TimeoutError, OSError) as e:
            self.logger.warning(f"获取群成员信息失败(网络错误): {e}")
        except (ImportError, RuntimeError) as e:
            self.logger.warning(f"获取群成员信息失败(系统错误): {e}")
        
        return None
    
    async def _get_fallback_nickname(self, event: AstrMessageEvent, user_id: str) -> str:
        """获取备用昵称
        
        当无法从群成员列表获取昵称时的备用方案,使用事件对象中的发送者名称.
        
        Args:
            event (AstrMessageEvent): AstrBot消息事件对象
            user_id (str): 用户ID
            
        Returns:
            str: 用户的显示名称,如果获取失败则返回 "用户{user_id}" 格式
            
        Raises:
            AttributeError: 当事件对象缺少必要属性时抛出
            KeyError: 当数据格式错误时抛出
            TypeError: 当参数类型错误时抛出
            
        Example:
            >>> nickname = await self._get_fallback_nickname(event, "123456")
            >>> print(nickname)
            '用户123456'
        """
        try:
            nickname = event.get_sender_name()
            # 确保昵称不为空或空字符串
            if not nickname or not nickname.strip():
                nickname = f"用户{user_id}"
                self.logger.warning(f"事件中获取的昵称为空，使用默认昵称: {nickname}")
            return nickname
        except (AttributeError, KeyError, TypeError) as e:
            self.logger.error(f"获取备用昵称失败: {e}")
            return f"用户{user_id}"

    @exception_handler(ExceptionConfig(log_exception=True, reraise=False))
    def clear_user_cache(self, user_id: str = None):
        """清理用户缓存"""
        if user_id:
            # 清理特定用户的缓存
            nickname_cache_key = f"nickname_{user_id}"
            if nickname_cache_key in self.user_nickname_cache:
                del self.user_nickname_cache[nickname_cache_key]
        else:
            # 清理所有用户缓存
            self.user_nickname_cache.clear()
        
        self.logger.info(f"清理用户缓存: {user_id or '全部'}")
    
    def _is_blocked_user(self, user_id: str) -> bool:
        """检查用户是否在屏蔽列表中
        
        Args:
            user_id (str): 用户ID
            
        Returns:
            bool: 如果用户在屏蔽列表中返回True，否则返回False
        """
        if not hasattr(self, 'plugin_config') or not self.plugin_config:
            return False
        
        blocked_users = getattr(self.plugin_config, 'blocked_users', [])
        if not blocked_users:
            return False
        
        # 将用户ID转换为字符串进行比较
        user_id_str = str(user_id)
        
        # 检查是否在屏蔽列表中
        return user_id_str in [str(uid) for uid in blocked_users]
    
    def _is_blocked_group(self, group_id: str) -> bool:
        """检查群聊是否在屏蔽列表中
        
        Args:
            group_id (str): 群聊ID
            
        Returns:
            bool: 如果群聊在屏蔽列表中返回True，否则返回False
        """
        if not hasattr(self, 'plugin_config') or not self.plugin_config:
            return False
        
        blocked_groups = getattr(self.plugin_config, 'blocked_groups', [])
        if not blocked_groups:
            return False
        
        # 将群聊ID转换为字符串进行比较
        group_id_str = str(group_id)
        
        # 检查是否在屏蔽列表中
        return group_id_str in [str(gid) for gid in blocked_groups]
    
    async def _get_group_members_cache(self, event: AstrMessageEvent, group_id: str) -> Optional[List[Dict[str, Any]]]:
        """获取群成员缓存"""
        cache_key = f"group_members_{group_id}"
        
        if cache_key in self.group_members_cache:
            return self.group_members_cache[cache_key]
        else:
            # 缓存未命中,从API获取
            return await self._fetch_group_members_from_api(event, group_id)
    
    async def _fetch_group_members_from_api(self, event: AstrMessageEvent, group_id: str) -> Optional[List[Dict[str, Any]]]:
        """从API获取群成员"""
        if self._get_platform_name(event) == "kook":
            return None
        if not hasattr(event, 'bot'):
            return None
        client = event.bot
        params = {"group_id": group_id}
        
        try:
            members_info = await client.api.call_action('get_group_member_list', **params)
            if members_info:
                # 缓存群成员列表,设置合理的过期时间
                cache_key = f"group_members_{group_id}"
                self.group_members_cache[cache_key] = members_info
                
                # 对于大群(成员数>500),记录警告
                if len(members_info) > 500:
                    self.logger.warning(f"群 {group_id} 成员数较多({len(members_info)}),建议调整缓存策略")
                
                return members_info
        except (AttributeError, KeyError, TypeError) as e:
            self.logger.warning(f"获取群成员列表失败(数据格式错误): {e}")
        except (ConnectionError, TimeoutError, OSError) as e:
            self.logger.warning(f"获取群成员列表失败(网络错误): {e}")
        except ImportError as e:
            self.logger.warning(f"获取群成员列表失败(导入错误): {e}")
        except RuntimeError as e:
            self.logger.warning(f"获取群成员列表失败(运行时错误): {e}")
        except ValueError as e:
            self.logger.warning(f"获取群成员列表失败(数据格式错误): {e}")
        
        return None

    async def _get_group_name(self, event: AstrMessageEvent, group_id: str) -> str:
        """获取群名称- 改进版本"""
        return f"群{group_id}"

    def _parse_roles_param(self, value: str) -> List[int]:
        parts = [p.strip() for p in (value or "").split(",") if p.strip()]
        roles: List[int] = []
        for part in parts:
            try:
                roles.append(int(part))
            except (TypeError, ValueError):
                continue
        return sorted(set(roles))

    def _parse_rank_query(self, event: AstrMessageEvent) -> Tuple[Optional[str], Optional[str], Optional[List[int]]]:
        """解析排行榜查询参数。

        支持:
        - #发言榜 [群号]
        - #发言榜 kook [群号] [roles=1,2]
        - #发言榜 qq [群号]
        - #发言榜 group_id=123
        - #发言榜 kook_guild_id=123 [roles=1,2]

        注意:
        - roles 参数仅允许在显式指定 kook（如 "kook" / "kook_guild_id="）时使用
        - roles=40572151,123 表示“只要包含任一 role 就参与排行”
        """
        message_str = getattr(event, 'message_str', '') or ''
        tokens = message_str.split()
        args = tokens[1:] if len(tokens) > 1 else []

        platform_hint: Optional[str] = None
        target_group_id: Optional[str] = None
        roles_filter: Optional[List[int]] = None

        for arg in args:
            if "=" in arg:
                key, value = arg.split("=", 1)
                key = key.lower().strip()
                value = value.strip()

                if key in ("group_id", "qq_group_id", "kook_group_id", "kook_guild_id", "guild_id"):
                    target_group_id = value
                    if key.startswith("kook") or key == "guild_id":
                        platform_hint = "kook"
                    elif key.startswith("qq"):
                        platform_hint = "qq"
                    continue

                if key == "roles":
                    roles_filter = self._parse_roles_param(value)
                    continue

            lower = arg.lower().strip()
            if lower in ("kook", "qq"):
                platform_hint = lower
                continue

            if target_group_id is None:
                target_group_id = arg.strip()

        return target_group_id, platform_hint, roles_filter
    
    async def _show_rank(self, event: AstrMessageEvent, rank_type: RankType):
        """显示排行榜 - 重构版本"""
        try:
            target_group_id, platform_hint, roles_filter = self._parse_rank_query(event)
            event_platform = self._get_platform_name(event)

            if roles_filter is not None:
                if platform_hint != "kook":
                    yield event.plain_result("roles 参数仅支持在 '#发言榜 kook ...' 查询下使用，例如: #发言榜 kook roles=40572151,123")
                    return
                if not roles_filter:
                    yield event.plain_result("roles 参数格式错误，例如: roles=40572151,123")
                    return

            if platform_hint and not target_group_id:
                if platform_hint == "kook":
                    target_group_id = DEFAULT_KOOK_GUILD_ID
                elif event_platform and platform_hint == event_platform:
                    target_group_id = self._resolve_stats_group_id(event)
                else:
                    yield event.plain_result("请提供目标群号，例如: #发言榜 kook 123456")
                    return
            
            if not target_group_id:
                target_group_id = self._resolve_stats_group_id(event)

            if not target_group_id:
                yield event.plain_result("无法获取群组信息，请在群聊中使用或指定群号")
                return

            try:
                target_group_id = Validators.validate_group_id(target_group_id)
            except ValidationError as e:
                yield event.plain_result(f"群号格式错误: {e}")
                return

            # 检查群聊是否在屏蔽列表中
            if self._is_blocked_group(str(target_group_id)):
                return
            
            # 准备数据
            rank_data = await self._prepare_rank_data(event, rank_type, target_group_id, roles_filter=roles_filter)
            if rank_data is None:
                if platform_hint == "kook":
                    all_groups = await self.data_manager.get_all_groups()
                    self.logger.info(
                        f"[KOOK][stats] no rank data for group_id={target_group_id}, "
                        f"known_groups={all_groups}"
                    )
                if roles_filter is not None:
                    yield event.plain_result("未找到符合 roles 条件的排行榜数据")
                else:
                    yield event.plain_result("无法获取排行榜数据,请检查群组信息或稍后重试")
                return
            
            group_id, current_user_id, filtered_data, config, title, group_info = rank_data
            
            # 根据配置选择显示模式
            if config.if_send_pic:
                async for result in self._render_rank_as_image(event, filtered_data, group_info, title, current_user_id, config):
                    yield result
            else:
                async for result in self._render_rank_as_text(event, filtered_data, group_info, title, config):
                    yield result
        
        except (IOError, OSError) as e:
            self.logger.error(f"文件操作失败: {e}")
            yield event.plain_result("文件操作失败,请检查权限")
        except (AttributeError, KeyError, TypeError) as e:
            self.logger.error(f"数据格式错误: {e}")
            yield event.plain_result("数据格式错误,请联系管理员")
        except (ConnectionError, TimeoutError) as e:
            self.logger.error(f"网络请求失败: {e}")
            yield event.plain_result("网络请求失败,请稍后重试")
        except ImportError as e:
            self.logger.error(f"导入错误: {e}")
            yield event.plain_result("系统错误,请联系管理员")
        except RuntimeError as e:
            self.logger.error(f"运行时错误: {e}")
            yield event.plain_result("系统错误,请联系管理员")
        except ValueError as e:
            self.logger.error(f"数据格式错误: {e}")
            yield event.plain_result("数据格式错误,请联系管理员")
    
    async def _prepare_rank_data(
        self,
        event: AstrMessageEvent,
        rank_type: RankType,
        target_group_id: Optional[str] = None,
        roles_filter: Optional[List[int]] = None,
    ):
        """准备排行榜数据"""
        # 获取群组ID和用户ID
        group_id = target_group_id or self._resolve_stats_group_id(event)
        current_user_id = event.get_sender_id()
        
        if not group_id:
            return None
            
        if not current_user_id:
            return None
        
        group_id = str(group_id)
        current_user_id = str(current_user_id)
        
        # 获取群组数据
        group_data = await self.data_manager.get_group_data(group_id)
        
        if not group_data:
            return None

        if roles_filter:
            group_data = [u for u in group_data if any(r in getattr(u, "roles", []) for r in roles_filter)]
            if not group_data:
                return None
        
        # 显示排行榜前强制刷新昵称缓存，确保昵称准确性
        await self._refresh_nickname_cache_for_ranking(event, group_id, group_data)
        
        # 根据类型筛选数据并获取排序值
        filtered_data_with_values = await self._filter_data_by_rank_type(group_data, rank_type)
        
        if not filtered_data_with_values:
            return None
        
        # 对数据进行排序
        filtered_data = sorted(filtered_data_with_values, key=lambda x: x[1], reverse=True)
        
        # 获取配置
        config = self.plugin_config
        
        # 生成标题
        title = self._generate_title(rank_type)
        
        # 创建群组信息
        group_info = GroupInfo(group_id=group_id)
        
        # 获取群名称
        group_name = await self._get_group_name(event, group_id)
        group_info.group_name = group_name
        
        return group_id, current_user_id, filtered_data, config, title, group_info
    
    async def _refresh_nickname_cache_for_ranking(self, event: AstrMessageEvent, group_id: str, group_data):
        """排行榜显示前强制刷新昵称缓存，确保显示最新昵称"""
        try:
            # 获取最新群成员信息
            members_info = await self._fetch_group_members_from_api(event, group_id)
            if not members_info:
                return
            
            # 重建群成员字典缓存
            dict_cache_key = f"group_members_dict_{group_id}"
            members_dict = {str(m.get("user_id", "")): m for m in members_info if m.get("user_id")}
            self.group_members_dict_cache[dict_cache_key] = members_dict
            
            # 更新用户数据中的昵称
            updated_count = 0
            for user in group_data:
                user_id = user.user_id
                if user_id in members_dict:
                    member = members_dict[user_id]
                    display_name = self._get_display_name_from_member(member)
                    if display_name and user.nickname != display_name:
                        # 更新昵称并同步到昵称缓存
                        old_nickname = user.nickname
                        user.nickname = display_name
                        updated_count += 1
                        
                        # 同时更新昵称缓存
                        nickname_cache_key = f"nickname_{user_id}"
                        self.user_nickname_cache[nickname_cache_key] = display_name
                        
                        if self.plugin_config.detailed_logging_enabled:
                            self.logger.debug(f"排行榜刷新昵称缓存: {old_nickname} → {display_name}")
            
            # 保存更新后的数据
            if updated_count > 0:
                await self.data_manager.save_group_data(group_id, group_data)
                if self.plugin_config.detailed_logging_enabled:
                    self.logger.info(f"排行榜显示前更新了 {updated_count} 个用户的昵称缓存")
            
        except (AttributeError, KeyError, TypeError, ValueError, RuntimeError, IOError, OSError, ConnectionError, asyncio.TimeoutError) as e:
            self.logger.warning(f"排行榜前刷新昵称缓存失败: {e}")

    async def _render_rank_as_image(self, event: AstrMessageEvent, filtered_data: List[tuple], 
                                  group_info: GroupInfo, title: str, current_user_id: str, config: PluginConfig):
        """渲染排行榜为图片模式"""
        temp_path = None
        try:
            # 提取用户数据用于图片生成，并应用人数限制
            # 先限制数量，再提取用户数据
            limited_data = filtered_data[:config.rand]
            users_for_image = []
            
            # 为用户数据设置display_total属性，确保图片生成器使用正确的数据
            # 修复：直接命令版排行榜图片显示错误数据的问题
            for user_data, count in limited_data:
                # 设置display_total属性（时间段内的发言数）
                user_data.display_total = count
                users_for_image.append(user_data)
            
            # 使用图片生成器
            temp_path = await self.image_generator.generate_rank_image(
                users_for_image, group_info, title, current_user_id
            )
            
            # 检查图片文件是否存在
            if await aiofiles.os.path.exists(temp_path):
                yield event.image_result(str(temp_path))
            else:
                # 回退到文字模式
                text_msg = self._generate_text_message(filtered_data, group_info, title, config)
                yield event.plain_result(text_msg)
                
        except (IOError, OSError, FileNotFoundError) as e:
            self.logger.error(f"生成图片失败: {e}")
            # 回退到文字模式
            text_msg = self._generate_text_message(filtered_data, group_info, title, config)
            yield event.plain_result(text_msg)
        except ImportError as e:
            self.logger.error(f"图片渲染失败(导入错误): {e}")
            # 回退到文字模式
            text_msg = self._generate_text_message(filtered_data, group_info, title, config)
            yield event.plain_result(text_msg)
        except RuntimeError as e:
            self.logger.error(f"图片渲染失败(运行时错误): {e}")
            # 回退到文字模式
            text_msg = self._generate_text_message(filtered_data, group_info, title, config)
            yield event.plain_result(text_msg)
        except ValueError as e:
            self.logger.error(f"图片渲染失败(数据格式错误): {e}")
            # 回退到文字模式
            text_msg = self._generate_text_message(filtered_data, group_info, title, config)
            yield event.plain_result(text_msg)
        finally:
            # 清理临时文件，避免资源泄漏
            if temp_path and await aiofiles.os.path.exists(temp_path):
                try:
                    await aiofiles.os.unlink(temp_path)
                except OSError as e:
                    self.logger.warning(f"清理临时图片文件失败: {temp_path}, 错误: {e}")
    
    async def _render_rank_as_text(self, event: AstrMessageEvent, filtered_data: List[tuple], 
                                 group_info: GroupInfo, title: str, config: PluginConfig):
        """渲染排行榜为文字模式"""
        text_msg = self._generate_text_message(filtered_data, group_info, title, config)
        yield event.plain_result(text_msg)
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _get_time_period_for_rank_type(self, rank_type: RankType) -> tuple:
        """获取排行榜类型对应的时间段
        
        Args:
            rank_type (RankType): 排行榜类型
            
        Returns:
            tuple: (start_date, end_date, period_name)，如果不需要时间段过滤则返回(None, None, None)
        """
        current_date = datetime.now().date()
        
        if rank_type == RankType.TOTAL:
            return None, None, "total"
        elif rank_type == RankType.DAILY:
            return current_date, current_date, "daily"
        elif rank_type == RankType.WEEKLY:
            # 获取本周开始日期(周一)
            days_since_monday = current_date.weekday()
            week_start = current_date - timedelta(days=days_since_monday)
            return week_start, current_date, "weekly"
        elif rank_type == RankType.MONTHLY:
            # 获取本月开始日期
            month_start = current_date.replace(day=1)
            return month_start, current_date, "monthly"
        elif rank_type == RankType.YEARLY:
            # 获取本年开始日期
            year_start = current_date.replace(month=1, day=1)
            return year_start, current_date, "yearly"
        elif rank_type == RankType.LAST_YEAR:
            # 获取去年的时间范围（1月1日 - 12月31日）
            last_year = current_date.year - 1
            year_start = date(last_year, 1, 1)
            year_end = date(last_year, 12, 31)
            return year_start, year_end, "lastyear"
        else:
            return None, None, "unknown"
    
    async def _filter_data_by_rank_type(self, group_data: List[UserData], rank_type: RankType) -> List[tuple]:
        """根据排行榜类型筛选数据并计算时间段内的发言次数 - 性能优化版本"""
        start_date, end_date, period_name = self._get_time_period_for_rank_type(rank_type)
        
        if rank_type == RankType.TOTAL:
            # 总榜：返回每个用户及其总发言数的元组，但过滤掉从未发言的用户和屏蔽用户
            return [(user, user.message_count) for user in group_data 
                   if user.message_count > 0 and not self._is_blocked_user(user.user_id)]
        
        # 时间段过滤：优化版本，使用预聚合策略减少双重循环
        # 策略：如果时间段较短（日榜），直接计算；如果时间段较长（周榜/月榜），使用缓存
        
        # 对于日榜，直接计算（因为时间段短，性能影响小）
        if rank_type == RankType.DAILY:
            return self._calculate_daily_rank(group_data, start_date, end_date)
        
        # 对于周榜和月榜，使用优化策略（现在是异步方法）
        elif rank_type in [RankType.WEEKLY, RankType.MONTHLY, RankType.YEARLY, RankType.LAST_YEAR]:
            return await self._calculate_period_rank_optimized(group_data, start_date, end_date)
        
        return []
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _calculate_daily_rank(self, group_data: List[UserData], start_date, end_date) -> List[tuple]:
        """计算日榜（直接计算策略）"""
        filtered_users = []
        for user in group_data:
            # 过滤屏蔽用户
            if self._is_blocked_user(user.user_id):
                continue
                
            if not user.history:
                continue
            
            # 计算指定时间段的发言次数
            period_count = user.get_message_count_in_period(start_date, end_date)
            if period_count > 0:
                filtered_users.append((user, period_count))
        
        return filtered_users
    
    async def _calculate_period_rank_optimized(self, group_data: List[UserData], start_date, end_date) -> List[tuple]:
        """计算周榜/月榜（优化策略）"""
        # 优化策略：先筛选出有历史记录的用户，然后批量计算
        active_users = [user for user in group_data if user.history]
        
        if not active_users:
            return []
        
        # 批量计算，减少函数调用开销
        filtered_users = []
        for user in active_users:
            # 过滤屏蔽用户
            if self._is_blocked_user(user.user_id):
                continue
                
            # 使用更高效的计算方法（现在是异步方法）
            period_count = await self._count_messages_in_period_fast(user.history, start_date, end_date)
            if period_count > 0:
                filtered_users.append((user, period_count))
        
        return filtered_users
    
    async def _count_messages_in_period_fast(self, history: List, start_date, end_date) -> int:
        """快速计算指定时间段内的消息数量（优化版本）
        
        如果历史记录未排序，将自动排序后进行计算。
        对于已排序的记录，使用高效的早停算法。
        """
        # 如果历史记录为空，直接返回0
        if not history:
            return 0
        
        # 完整遍历检查列表是否真正有序，避免采样检查的误判问题
        is_sorted = True
        if len(history) > 1:
            try:
                # 完整遍历检查：确保列表真正有序（优化版本）
                for current_item, next_item in zip(history[:-1], history[1:]):
                    current_date = current_item.to_date() if hasattr(current_item, 'to_date') else current_item
                    next_date = next_item.to_date() if hasattr(next_item, 'to_date') else next_item
                    if current_date > next_date:
                        is_sorted = False
                        break
                        
            except (AttributeError, TypeError):
                # 如果无法比较，假设未排序
                is_sorted = False
        
        # 如果检测到列表确实有序，使用早停算法
        if is_sorted:
            count = 0
            for hist_date in history:
                # 转换为日期对象
                hist_date_obj = hist_date.to_date() if hasattr(hist_date, 'to_date') else hist_date
                
                # 检查是否在指定时间段内
                if hist_date_obj < start_date:
                    continue
                if hist_date_obj > end_date:
                    # 已排序，可以提前跳出循环
                    break
                count += 1
            
            return count
        
        # 如果检测到列表无序，直接使用无序版本计算
        else:
            return self._count_messages_in_period_unordered(history, start_date, end_date)
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _count_messages_in_period_unordered(self, history: List, start_date, end_date) -> int:
        """计算指定时间段内的消息数量（适用于未排序的历史记录）"""
        if not history:
            return 0
        
        count = 0
        for hist_date in history:
            hist_date_obj = hist_date.to_date() if hasattr(hist_date, 'to_date') else hist_date
            if start_date <= hist_date_obj <= end_date:
                count += 1
        
        return count
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _generate_title(self, rank_type: RankType) -> str:
        """生成标题"""
        now = datetime.now()
        
        if rank_type == RankType.TOTAL:
            return "总发言排行榜"
        elif rank_type == RankType.DAILY:
            return f"今日[{now.year}年{now.month}月{now.day}日]发言榜单"
        elif rank_type == RankType.WEEKLY:
            # 计算周数
            week_num = now.isocalendar().week
            return f"本周[{now.year}年{now.month}月第{week_num}周]发言榜单"
        elif rank_type == RankType.MONTHLY:
            return f"本月[{now.year}年{now.month}月]发言榜单"
        elif rank_type == RankType.YEARLY:
            return f"本年[{now.year}年]发言榜单"
        elif rank_type == RankType.LAST_YEAR:
            last_year = now.year - 1
            return f"去年[{last_year}年]发言榜单"
        else:
            return "发言榜单"
    
    def _generate_text_message(self, users_with_values: List[tuple], group_info: GroupInfo, title: str, config: PluginConfig) -> str:
        """生成文字消息
        
        Args:
            users_with_values: 包含(UserData, sort_value)元组的列表
            group_info: 群组信息
            title: 排行榜标题
            config: 插件配置
            
        Returns:
            str: 格式化的文字消息
        """
        # 计算时间段内的总发言数
        total_messages = sum(sort_value for _, sort_value in users_with_values)
        
        # 数据已经在_show_rank中排好序，直接使用并限制数量
        top_users = users_with_values[:config.rand]
        
        msg = [f"{title}\n发言总数: {total_messages}\n━━━━━━━━━━━━━━\n"]
        
        for i, (user, user_messages) in enumerate(top_users):
            # 使用时间段内的发言数计算百分比
            percentage = ((user_messages / total_messages) * 100) if total_messages > 0 else 0
            msg.append(f"{i + 1}-{user.nickname}·{user_messages}次({percentage:.2f}%)\n")
        
        return ''.join(msg)
    
    # ========== 定时功能管理命令 ==========
    
    @filter.command("发言榜定时状态")
    async def timer_status(self, event: AstrMessageEvent):
        """查看定时任务状态"""
        try:
            # 获取当前配置（使用转换后的配置）
            config = self.plugin_config
            
            # 构建状态信息
            status_lines = [
                "📊 定时任务状态",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                "",
                "🔧 基础设置",
                f"┌─ 定时功能: {'✅ 已启用' if config.timer_enabled else '❌ 已禁用'}",
                f"├─ 推送时间: {config.timer_push_time}",
                f"├─ 排行榜类型: {self._get_rank_type_text(config.timer_rank_type)}",
                f"├─ 推送模式: {'图片' if config.if_send_pic else '文字'}",
                f"└─ 显示人数: {config.rand} 人",
                "",
                "🎯 目标群组"
            ]
            
            # 添加目标群组信息
            if config.timer_target_groups:
                for i, group_id in enumerate(config.timer_target_groups, 1):
                    origin_status = "✅" if str(group_id) in self.group_unified_msg_origins else "❌"
                    status_lines.append(f"┌─ {i}. {group_id} {origin_status}")
                
                # 添加unified_msg_origin说明
                status_lines.append("└─ 💡 unified_msg_origin状态: ✅已收集/❌未收集")
                status_lines.append("   (❌状态需在群组发送消息收集)")
            else:
                status_lines.append("┌─ ⚠️ 未设置任何目标群组")
                status_lines.append("└─ 💡 使用 #设置定时群组 添加群组")
            
            # 添加定时任务状态
            if self.timer_manager:
                timer_status = await self.timer_manager.get_status()
                status_lines.extend([
                    "",
                    "⏰ 任务状态",
                    f"┌─ 运行状态: {self._get_status_text(timer_status['status'])}",
                    f"├─ 下次推送: {timer_status['next_push_time'] or '未设置'}",
                    f"└─ 剩余时间: {timer_status['time_until_next'] or 'N/A'}"
                ])
            
            yield event.plain_result('\n'.join(status_lines))
            
        except (IOError, OSError, KeyError) as e:
            self.logger.error(f"获取定时状态失败: {e}")
            yield event.plain_result("获取定时状态失败，请稍后重试！")
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"获取定时状态失败(运行时错误): {e}")
            yield event.plain_result("获取定时状态失败，请稍后重试！")
    
    @filter.command("手动推送发言榜")
    async def manual_push(self, event: AstrMessageEvent):
        """手动推送排行榜"""
        try:
            if not self.timer_manager:
                yield event.plain_result("定时管理器未初始化，无法执行手动推送！")
                return
            
            # 检查TimerManager是否有有效的context
            if not hasattr(self.timer_manager, 'context') or not self.timer_manager.context:
                yield event.plain_result("❌ 定时管理器未完全初始化！\n\n💡 可能的原因：\n• 插件初始化过程中出现异常\n• 上下文信息缺失\n\n🔧 解决方案：\n• 重启机器人或重新加载插件\n• 检查插件配置是否正确")
                return
            
            # 使用当前转换的配置而不是从文件读取
            config = self.plugin_config
            
            if not config.timer_target_groups:
                yield event.plain_result("未设置目标群组，请先使用 #设置定时群组 设置目标群组！")
                return
            
            # 执行手动推送
            yield event.plain_result("正在执行手动推送，请稍候...")
            
            success = await self.timer_manager.manual_push(config)
            
            if success:
                yield event.plain_result("✅ 手动推送执行成功！")
            else:
                yield event.plain_result("❌ 手动推送执行失败！\n\n💡 可能的原因：\n• 缺少 unified_msg_origin\n• 群组权限不足\n\n🔧 解决方案：\n• 在群组中发送任意消息以收集 unified_msg_origin\n• 检查机器人是否有群组发言权限")
            
        except (AttributeError, TypeError) as e:
            self.logger.error(f"处理手动推送请求失败: {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
        except (RuntimeError, ValueError, KeyError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"处理手动推送请求失败(运行时错误): {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
    
    @filter.command("设置发言榜定时时间")
    async def set_timer_time(self, event: AstrMessageEvent):
        """设置定时推送时间
        
        自动设置当前群组为定时群组并启用定时功能
        """
        try:
            # 获取参数
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            if not args:
                yield event.plain_result("请指定时间！用法:#设置定时时间 16:12")
                return
            
            time_str = args[0]
            
            # 验证时间格式
            if not self._validate_time_format(time_str):
                yield event.plain_result("时间格式错误！请使用 HH:MM 格式，例如：16:12")
                return
            
            # 获取当前群组ID
            group_id = event.get_group_id()
            if not group_id:
                yield event.plain_result("无法获取当前群组ID！")
                return
            
            # 获取当前配置（使用转换后的配置）
            config = self.plugin_config
            config.timer_push_time = time_str
            
            # 自动设置当前群组为定时群组
            if str(group_id) not in config.timer_target_groups:
                config.timer_target_groups.append(str(group_id))
            
            # 自动启用定时功能
            config.timer_enabled = True
            
            # 更新定时任务
            rank_type_text = self._get_rank_type_text(config.timer_rank_type)
            if self.timer_manager:
                success = await self.timer_manager.update_config(config, self.group_unified_msg_origins)
                if success:
                    yield event.plain_result(
                        f"✅ 定时推送设置完成！\n"
                        f"• 推送时间：{time_str}\n"
                        f"• 目标群组：{group_id}\n"
                        f"• 排行榜类型：{rank_type_text}\n"
                        f"• 状态：已启用\n\n"
                        f"💡 提示：如果推送失败，请在群组中发送任意消息以收集unified_msg_origin"
                    )
                else:
                    yield event.plain_result(
                        f"⚠️ 定时推送设置部分完成！\n"
                        f"• 推送时间：{time_str}\n"
                        f"• 目标群组：{group_id}\n"
                        f"• 排行榜类型：{rank_type_text}\n"
                        f"• 状态：配置保存成功，但定时任务启动失败\n\n"
                        f"💡 提示：如果推送失败，请在群组中发送任意消息以收集unified_msg_origin"
                    )
            else:
                yield event.plain_result(f"✅ 定时推送配置已保存！\n• 推送时间：{time_str}\n• 目标群组：{group_id}\n• 排行榜类型：{rank_type_text}\n• 状态：配置保存成功\n\n💡 提示：定时管理器未初始化，请检查插件配置")
            
        except ValueError as e:
            self.logger.error(f"处理设置定时时间请求失败: {e}")
            yield event.plain_result("时间格式错误，请使用 HH:MM 格式！")
        except (IOError, OSError) as e:
            self.logger.error(f"处理设置定时时间请求失败: {e}")
            yield event.plain_result("保存配置失败，请稍后重试！")
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"处理设置定时时间请求失败(运行时错误): {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
    
    @filter.command("设置发言榜定时群组")
    async def set_timer_groups(self, event: AstrMessageEvent):
        """设置定时推送目标群组"""
        try:
            # 获取参数
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            if not args:
                yield event.plain_result("请指定群组ID！用法:#设置发言榜定时群组 123456789 987654321")
                return
            
            # 验证群组ID
            valid_groups = []
            for group_id in args:
                if group_id.isdigit() and len(group_id) >= 5:
                    valid_groups.append(group_id)
                else:
                    yield event.plain_result(f"群组ID格式错误: {group_id}，必须是5位以上数字")
                    return
            
            # 获取当前配置（使用转换后的配置）
            config = self.plugin_config
            config.timer_target_groups = valid_groups
            
            # 更新定时任务
            if self.timer_manager and config.timer_enabled:
                await self.timer_manager.update_config(config, self.group_unified_msg_origins)
            
            groups_text = "\n".join([f"   • {group_id}" for group_id in valid_groups])
            yield event.plain_result(f"✅ 定时推送目标群组已设置：\n{groups_text}")
            
        except ValueError as e:
            self.logger.error(f"处理设置定时群组请求失败: {e}")
            yield event.plain_result("群组ID格式错误，请输入有效的群组ID！")
        except (IOError, OSError) as e:
            self.logger.error(f"处理设置定时群组请求失败: {e}")
            yield event.plain_result("保存配置失败，请稍后重试！")
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"处理设置定时群组请求失败(运行时错误): {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
    
    @filter.command("删除发言榜定时群组")
    async def remove_timer_groups(self, event: AstrMessageEvent):
        """删除定时推送目标群组"""
        try:
            # 获取参数
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            # 获取当前配置（使用转换后的配置）
            config = self.plugin_config
            current_groups = config.timer_target_groups
            
            if not args:
                # 清空所有定时群组
                config.timer_target_groups = []
                
                # 更新定时任务
                if self.timer_manager and config.timer_enabled:
                    await self.timer_manager.update_config(config, self.group_unified_msg_origins)
                
                yield event.plain_result("✅ 已清空所有定时推送目标群组")
                return
            
            # 删除指定群组
            groups_to_remove = []
            invalid_groups = []
            
            for group_id in args:
                if group_id.isdigit() and len(group_id) >= 5:
                    groups_to_remove.append(group_id)
                else:
                    invalid_groups.append(group_id)
            
            if invalid_groups:
                yield event.plain_result(f"群组ID格式错误: {', '.join(invalid_groups)}，必须是5位以上数字")
                return
            
            # 从当前群组列表中移除指定群组
            remaining_groups = [group for group in current_groups if group not in groups_to_remove]
            
            # 保存配置
            config.timer_target_groups = remaining_groups
            await self.data_manager.save_config(config)
            
            # 更新定时任务
            if self.timer_manager and config.timer_enabled:
                await self.timer_manager.update_config(config, self.group_unified_msg_origins)
            
            if groups_to_remove:
                removed_text = "\n".join([f"   • {group_id}" for group_id in groups_to_remove])
                remaining_text = "\n".join([f"   • {group_id}" for group_id in remaining_groups]) if remaining_groups else "   无"
                yield event.plain_result(f"✅ 已删除定时推送目标群组：\n{removed_text}\n\n📋 剩余群组：\n{remaining_text}")
            else:
                yield event.plain_result("⚠️ 未找到要删除的群组")
            
        except ValueError as e:
            self.logger.error(f"处理删除定时群组请求失败: {e}")
            yield event.plain_result("群组ID格式错误，请输入有效的群组ID！")
        except (IOError, OSError) as e:
            self.logger.error(f"处理删除定时群组请求失败: {e}")
            yield event.plain_result("保存配置失败，请稍后重试！")
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"处理删除定时群组请求失败(运行时错误): {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
    
    @filter.command("启用发言榜定时")
    async def enable_timer(self, event: AstrMessageEvent):
        """启用定时推送功能"""
        try:
            # 获取当前配置（使用转换后的配置）
            config = self.plugin_config
            
            # 检查配置
            if not config.timer_target_groups:
                yield event.plain_result("请先设置目标群组！用法:#设置定时群组 群组ID")
                return
            
            # 启用定时功能
            config.timer_enabled = True
            
            # 更新定时任务（使用update_config确保group_unified_msg_origins被正确传递）
            if self.timer_manager:
                # 检查TimerManager是否有有效的context
                if not hasattr(self.timer_manager, 'context') or not self.timer_manager.context:
                    yield event.plain_result("⚠️ 定时管理器未完全初始化！\n\n💡 可能的原因：\n• 插件初始化过程中出现异常\n• 上下文信息缺失\n\n🔧 解决方案：\n• 重启机器人或重新加载插件\n• 检查插件配置是否正确")
                    return
                
                success = await self.timer_manager.update_config(config, self.group_unified_msg_origins)
                if success:
                    yield event.plain_result("✅ 定时推送功能已启用！")
                else:
                    yield event.plain_result("⚠️ 定时推送功能启用失败，请检查配置！")
            else:
                yield event.plain_result("⚠️ 定时管理器未初始化！")
            
        except (IOError, OSError) as e:
            self.logger.error(f"处理启用定时请求失败: {e}")
            yield event.plain_result("保存配置失败，请稍后重试！")
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"处理启用定时请求失败(运行时错误): {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
    
    @filter.command("禁用发言榜定时")
    async def disable_timer(self, event: AstrMessageEvent):
        """禁用定时推送功能"""
        try:
            # 获取当前配置（使用转换后的配置）
            config = self.plugin_config
            
            # 禁用定时功能
            config.timer_enabled = False
            
            # 停止定时任务
            if self.timer_manager:
                await self.timer_manager.stop_timer()
            
            yield event.plain_result("✅ 定时推送功能已禁用！")
            
        except (IOError, OSError) as e:
            self.logger.error(f"处理禁用定时请求失败: {e}")
            yield event.plain_result("保存配置失败，请稍后重试！")
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"处理禁用定时请求失败(运行时错误): {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
    
    @filter.command("设置发言榜定时类型")
    async def set_timer_type(self, event: AstrMessageEvent):
        """设置定时推送的排行榜类型"""
        try:
            # 获取参数
            args = event.message_str.split()[1:] if hasattr(event, 'message_str') else []
            
            if not args:
                yield event.plain_result("请指定排行榜类型！用法:#设置定时类型 total/daily/week/month")
                return
            
            rank_type = args[0].lower()
            
            # 验证排行榜类型
            valid_types = ['total', 'daily', 'week', 'weekly', 'month', 'monthly']
            if rank_type not in valid_types:
                yield event.plain_result(f"排行榜类型错误！可用类型: {', '.join(valid_types)}")
                return
            
            # 获取当前配置（使用转换后的配置）
            config = self.plugin_config
            config.timer_rank_type = rank_type
            
            # 更新定时任务
            if self.timer_manager and config.timer_enabled:
                await self.timer_manager.update_config(config, self.group_unified_msg_origins)
            
            type_text = self._get_rank_type_text(rank_type)
            yield event.plain_result(f"✅ 定时推送排行榜类型已设置为 {type_text}！")
            
        except ValueError as e:
            self.logger.error(f"处理设置定时类型请求失败: {e}")
            yield event.plain_result("排行榜类型错误，请使用：total/daily/weekly/monthly")
        except (IOError, OSError) as e:
            self.logger.error(f"处理设置定时类型请求失败: {e}")
            yield event.plain_result("保存配置失败，请稍后重试！")
        except (RuntimeError, AttributeError, ValueError, TypeError, ConnectionError, asyncio.TimeoutError) as e:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"处理设置定时类型请求失败(运行时错误): {e}")
            yield event.plain_result("处理请求失败，请稍后重试！")
    
    # ========== 辅助方法 ==========
    
    def _handle_command_exception(self, event: AstrMessageEvent, operation_name: str, exception: Exception) -> bool:
        """公共的异常处理方法，减少代码重复
        
        Args:
            event: 消息事件对象
            operation_name: 操作名称，用于日志记录
            exception: 异常对象
            
        Returns:
            bool: 是否成功处理了异常
        """
        try:
            if isinstance(exception, (KeyError, TypeError)):
                self.logger.error(f"{operation_name}失败(数据格式错误): {exception}", exc_info=True)
                event.plain_result(f"{operation_name}失败，请稍后重试")
                return True
            elif isinstance(exception, (IOError, OSError, FileNotFoundError)):
                self.logger.error(f"{operation_name}失败(文件操作错误): {exception}", exc_info=True)
                event.plain_result(f"{operation_name}失败，请稍后重试")
                return True
            elif isinstance(exception, ValueError):
                self.logger.error(f"{operation_name}失败(参数错误): {exception}", exc_info=True)
                event.plain_result(f"{operation_name}失败，请稍后重试")
                return True
            elif isinstance(exception, RuntimeError):
                self.logger.error(f"{operation_name}失败(运行时错误): {exception}", exc_info=True)
                event.plain_result(f"{operation_name}失败，请稍后重试")
                return True
            else:
                self.logger.error(f"{operation_name}失败(未预期的错误类型 {type(exception).__name__}): {exception}", exc_info=True)
                event.plain_result(f"{operation_name}失败，请稍后重试")
                return True
        except (RuntimeError, AttributeError, ValueError, TypeError, KeyError) as handler_error:
            # 修复：替换过于宽泛的Exception为具体异常类型
            self.logger.error(f"异常处理器本身出错: {handler_error}", exc_info=True)
            return False
    
    def _log_operation_result(self, operation_name: str, success: bool, details: str = ""):
        """公共的操作结果日志记录方法，减少代码重复
        
        Args:
            operation_name: 操作名称
            success: 是否成功
            details: 详细信息
        """
        if success:
            self.logger.info(f"{operation_name}成功{details}")
        else:
            self.logger.warning(f"{operation_name}失败{details}")
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _get_status_text(self, status: str) -> str:
        """获取状态文本"""
        status_mapping = {
            'stopped': '已停止',
            'running': '运行中',
            'error': '错误',
            'paused': '已暂停'
        }
        return status_mapping.get(status, status)
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _format_datetime(self, dt_str: str) -> str:
        """格式化日期时间"""
        if not dt_str:
            return '未设置'
        
        try:
            # 解析ISO格式的时间字符串
            dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            return dt.strftime('%m月%d日 %H:%M')
        except (ValueError, TypeError):
            # 修复：替换过于宽泛的except:为具体异常类型
            return dt_str
    
    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _validate_time_format(self, time_str: str) -> bool:
        """验证时间格式"""
        # 使用模块级别导入的 re 模块
        pattern = r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$'
        return bool(re.match(pattern, time_str))
    

    @exception_handler(ExceptionConfig(log_exception=True, reraise=True))
    def _get_rank_type_text(self, rank_type: str) -> str:
        """获取排行榜类型的中文描述
        
        Args:
            rank_type: 排行榜类型字符串
            
        Returns:
            str: 排行榜类型的中文描述
        """
        type_mapping = {
            'total': '总排行榜',
            'daily': '今日排行榜', 
            'week': '本周排行榜',
            'weekly': '本周排行榜',
            'month': '本月排行榜',
            'monthly': '本月排行榜'
        }
        return type_mapping.get(rank_type, rank_type)
