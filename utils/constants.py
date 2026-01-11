"""
常量定义模块

集中管理插件中使用的所有常量，避免重复定义。
"""

# ========== 缓存配置常量 ==========
# 这些常量控制缓存的行为，修改这些值会影响整个插件的缓存性能

DATA_CACHE_MAXSIZE = 1000  # 数据缓存最大容量，用于缓存群组数据
DATA_CACHE_TTL = 300  # 数据缓存生存时间（秒），5分钟后过期
CONFIG_CACHE_MAXSIZE = 10  # 配置缓存最大容量，用于缓存插件配置
CONFIG_CACHE_TTL = 60  # 配置缓存生存时间（秒），1分钟后过期

# ========== 排行榜配置常量 ==========
MAX_RANK_COUNT = 100  # 排行榜最大显示人数
RANK_COUNT_MIN = 1  # 排行榜最小显示人数
RANK_COUNT_DEFAULT = 20  # 排行榜默认显示人数

# ========== 用户昵称缓存配置 ==========
USER_NICKNAME_CACHE_TTL = 300  # 用户昵称缓存TTL（秒），5分钟
USER_NICKNAME_CACHE_MAXSIZE = 500  # 用户昵称缓存最大容量

# ========== 群成员缓存配置 ==========
GROUP_MEMBERS_CACHE_TTL = 300  # 群成员缓存TTL（秒），5分钟
GROUP_MEMBERS_CACHE_MAXSIZE = 100  # 群成员缓存最大容量

# ========== 图片生成配置 ==========
IMAGE_WIDTH = 1200  # 生成图片宽度（像素）
VIEWPORT_HEIGHT = 1  # 初始视口高度
BROWSER_TIMEOUT = 10000  # 浏览器超时时间（毫秒）
DEFAULT_FONT_SIZE = 14  # 默认字体大小
ROW_HEIGHT = 30  # 行高

# ========== 验证常量 ==========
GROUP_ID_MIN_LENGTH = 5  # 群组ID最小长度
GROUP_ID_MAX_LENGTH = 32  # 群组ID最大长度
USER_ID_MIN_LENGTH = 1  # 用户ID最小长度
USER_ID_MAX_LENGTH = 20  # 用户ID最大长度
NICKNAME_MAX_LENGTH = 50  # 昵称最大长度
MESSAGE_CONTENT_MAX_LENGTH = 200  # 消息内容最大长度
FILE_PATH_MAX_LENGTH = 500  # 文件路径最大长度

# ========== 报告类型 ==========
VALID_REPORT_TYPES = ['daily', 'weekly', 'monthly']

# ========== 图片模式 ==========
IMAGE_MODE_TEXT = 0  # 文字模式
IMAGE_MODE_IMAGE = 1  # 图片模式
TEXT_MODE_STRINGS = ['0', 'false', '否', '关', '关闭', '文字', 'text']
IMAGE_MODE_STRINGS = ['1', 'true', '是', '开', '开启', '图片', 'image', '图片模式']

# ========== 排行榜验证 ==========
RANK_LIMIT_MIN = 5
RANK_LIMIT_MAX = 50
RANK_LIMIT_DEFAULT = 20

# ========== 危险字符列表 ==========
DANGEROUS_CHARS = ['<', '>', ':', '"', '|', '?', '*', '\x00', '\x0a', '\x0d']
