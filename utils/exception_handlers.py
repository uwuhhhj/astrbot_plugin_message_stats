"""
异常处理装饰器模块

提供通用的异常处理装饰器，用于简化函数中的异常处理逻辑。
支持多种异常类型和不同的返回策略。
"""

import asyncio
import functools
import traceback
from typing import Any, Callable, Optional, Union, Dict, List, Tuple
from astrbot.api import logger as astrbot_logger


class ExceptionHandler:
    """异常处理器类
    
    提供统一的异常处理机制，支持多种异常类型和返回策略。
    """
    
    logger = astrbot_logger
    
    @staticmethod
    def handle_io_error(func_name: str, error: Exception, default_return: Any = None) -> Any:
        """处理IO和系统相关异常
        
        Args:
            func_name: 函数名称
            error: 异常对象
            default_return: 默认返回值
            
        Returns:
            默认返回值
        """
        ExceptionHandler.logger.error(f"{func_name} IO/系统错误: {error}")
        return default_return
    
    @staticmethod
    def handle_data_error(func_name: str, error: Exception, default_return: Any = None) -> Any:
        """处理数据格式相关异常
        
        Args:
            func_name: 函数名称
            error: 异常对象
            default_return: 默认返回值
            
        Returns:
            默认返回值
        """
        ExceptionHandler.logger.error(f"{func_name} 数据格式错误: {error}")
        return default_return
    
    @staticmethod
    def handle_validation_error(func_name: str, error: Exception, default_return: Any = None) -> Any:
        """处理验证相关异常
        
        Args:
            func_name: 函数名称
            error: 异常对象
            default_return: 默认返回值
            
        Returns:
            默认返回值
        """
        ExceptionHandler.logger.error(f"{func_name} 验证错误: {error}")
        return default_return
    
    @staticmethod
    def handle_runtime_error(func_name: str, error: Exception, default_return: Any = None) -> Any:
        """处理运行时异常
        
        Args:
            func_name: 函数名称
            error: 异常对象
            default_return: 默认返回值
            
        Returns:
            默认返回值
        """
        ExceptionHandler.logger.error(f"{func_name} 运行时错误: {error}")
        ExceptionHandler.logger.debug(f"详细错误信息: {traceback.format_exc()}")
        return default_return
    
    @staticmethod
    def handle_timeout_error(func_name: str, error: Exception, default_return: Any = None) -> Any:
        """处理超时异常
        
        Args:
            func_name: 函数名称
            error: 异常对象
            default_return: 默认返回值
            
        Returns:
            默认返回值
        """
        ExceptionHandler.logger.error(f"{func_name} 超时错误: {error}")
        return default_return


def safe_execute(default_return=None, log_level="error", include_traceback=True):
    """通用异常处理装饰器
    
    为函数添加异常处理功能，自动捕获和处理常见异常类型。
    
    Args:
        default_return: 异常时的默认返回值
        log_level: 日志级别 ("error", "warning", "info")
        include_traceback: 是否包含详细错误堆栈信息
        
    Returns:
        装饰后的函数
        
    Example:
        @safe_execute(default_return=[], log_level="error")
        async def get_data():
            # 可能抛出异常的代码
            pass
            
        @safe_execute(default_return=None, log_level="warning")
        def process_data():
            # 可能抛出异常的代码
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except (IOError, OSError) as e:
                return ExceptionHandler.handle_io_error(func.__name__, e, default_return)
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                return ExceptionHandler.handle_data_error(func.__name__, e, default_return)
            except (RuntimeError, NotImplementedError) as e:
                return ExceptionHandler.handle_runtime_error(func.__name__, e, default_return)
            except asyncio.TimeoutError as e:
                return ExceptionHandler.handle_timeout_error(func.__name__, e, default_return)
            except Exception as e:
                # 捕获所有其他异常
                log_message = f"{func.__name__} 发生未知错误: {e}"
                if include_traceback:
                    log_message += f"\n{traceback.format_exc()}"
                
                if log_level == "error":
                    ExceptionHandler.logger.error(log_message)
                elif log_level == "warning":
                    ExceptionHandler.logger.warning(log_message)
                elif log_level == "info":
                    ExceptionHandler.logger.info(log_message)
                
                return default_return
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (IOError, OSError) as e:
                return ExceptionHandler.handle_io_error(func.__name__, e, default_return)
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                return ExceptionHandler.handle_data_error(func.__name__, e, default_return)
            except (RuntimeError, NotImplementedError) as e:
                return ExceptionHandler.handle_runtime_error(func.__name__, e, default_return)
            except Exception as e:
                # 捕获所有其他异常
                log_message = f"{func.__name__} 发生未知错误: {e}"
                if include_traceback:
                    log_message += f"\n{traceback.format_exc()}"
                
                if log_level == "error":
                    ExceptionHandler.logger.error(log_message)
                elif log_level == "warning":
                    ExceptionHandler.logger.warning(log_message)
                elif log_level == "info":
                    ExceptionHandler.logger.info(log_message)
                
                return default_return
        
        # 根据函数类型返回相应的包装器
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


def safe_execute_with_context(default_return=None, context_field="logger"):
    """带上下文的异常处理装饰器
    
    使用函数自身的logger或其他上下文字段进行日志记录。
    
    Args:
        default_return: 异常时的默认返回值
        context_field: 上下文字段名，默认为"logger"
        
    Returns:
        装饰后的函数
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except (IOError, OSError) as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} IO/系统错误: {e}")
                return default_return
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} 数据格式错误: {e}")
                return default_return
            except (RuntimeError, NotImplementedError) as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} 运行时错误: {e}")
                return default_return
            except asyncio.TimeoutError as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} 超时错误: {e}")
                return default_return
            except Exception as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} 发生未知错误: {e}\n{traceback.format_exc()}")
                return default_return
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (IOError, OSError) as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} IO/系统错误: {e}")
                return default_return
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} 数据格式错误: {e}")
                return default_return
            except (RuntimeError, NotImplementedError) as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} 运行时错误: {e}")
                return default_return
            except Exception as e:
                logger = getattr(func, context_field, ExceptionHandler.logger)
                logger.error(f"{func.__name__} 发生未知错误: {e}\n{traceback.format_exc()}")
                return default_return
        
        # 根据函数类型返回相应的包装器
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# 预定义的装饰器工厂函数，用于简化使用
def safe_data_operation(default_return=None):
    """数据操作安全装饰器
    
    专门用于数据读写操作的异常处理装饰器。
    
    Args:
        default_return: 异常时的默认返回值
        
    Returns:
        装饰后的函数
    """
    return safe_execute(default_return=default_return, log_level="error")


def safe_file_operation(default_return=None):
    """文件操作安全装饰器
    
    专门用于文件读写操作的异常处理装饰器。
    
    Args:
        default_return: 异常时的默认返回值
        
    Returns:
        装饰后的函数
    """
    return safe_execute(default_return=default_return, log_level="error")


def safe_cache_operation(default_return=None):
    """缓存操作安全装饰器
    
    专门用于缓存操作的异常处理装饰器。
    
    Args:
        default_return: 异常时的默认返回值
        
    Returns:
        装饰后的函数
    """
    return safe_execute(default_return=default_return, log_level="warning")


def safe_config_operation(default_return=None):
    """配置操作安全装饰器
    
    专门用于配置管理的异常处理装饰器。
    
    Args:
        default_return: 异常时的默认返回值
        
    Returns:
        装饰后的函数
    """
    return safe_execute(default_return=default_return, log_level="error")


def safe_calculation(default_return=None):
    """计算操作安全装饰器
    
    专门用于数据计算和统计的异常处理装饰器。
    
    Args:
        default_return: 异常时的默认返回值
        
    Returns:
        装饰后的函数
    """
    return safe_execute(default_return=default_return, log_level="warning")


def safe_generation(default_return=None):
    """生成操作安全装饰器
    
    专门用于图片生成、HTML渲染等生成操作的异常处理装饰器。
    
    Args:
        default_return: 异常时的默认返回值
        
    Returns:
        装饰后的函数
    """
    return safe_execute(default_return=default_return, log_level="error")


def safe_timer_operation(default_return=False):
    """定时任务安全装饰器
    
    专门用于定时任务操作的异常处理装饰器。
    
    Args:
        default_return: 异常时的默认返回值
        
    Returns:
        装饰后的函数
    """
    return safe_execute(default_return=default_return, log_level="error")

# ========== 兼容性别名装饰器 ==========

def exception_handler(config=None, **kwargs):
    """通用异常处理装饰器
    
    Args:
        config: ExceptionConfig对象或None
        **kwargs: 其他配置参数
        
    Returns:
        装饰后的函数
    """
    if config is None:
        config = ExceptionConfig(**kwargs)
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                ExceptionHandler.logger.error(f"函数 {func.__name__} 执行异常: {e}")
                if config.reraise:
                    raise
                return config.default_return
        return wrapper
    return decorator


def data_operation_handler(operation_name: str, description: str = ""):
    """数据操作异常处理装饰器
    
    Args:
        operation_name: 操作名称
        description: 操作描述
        
    Returns:
        装饰后的函数
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                ExceptionHandler.logger.error(f"数据操作 {operation_name} 失败: {e}")
                if description:
                    ExceptionHandler.logger.error(f"操作描述: {description}")
                return None
        return wrapper
    return decorator


def file_operation_handler(operation_name: str = "文件操作"):
    """文件操作异常处理装饰器
    
    Args:
        operation_name: 操作名称
        
    Returns:
        装饰后的函数
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                ExceptionHandler.logger.error(f"{operation_name} 失败: {e}")
                return None
        return wrapper
    return decorator


def log_exception(func):
    """日志记录装饰器
    
    专门用于记录异常信息的装饰器。
    
    Args:
        func: 被装饰的函数
        
    Returns:
        装饰后的函数
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            ExceptionHandler.logger.error(f"函数 {func.__name__} 异常: {e}")
            ExceptionHandler.logger.error(f"异常详情: {traceback.format_exc()}")
            raise
    return wrapper


class ExceptionConfig:
    """异常处理配置类
    
    用于配置异常处理装饰器的行为。
    """
    
    def __init__(
        self, 
        default_return=None, 
        log_level="error", 
        log_exception=True, 
        reraise=False,
        operation_name=""
    ):
        self.default_return = default_return
        self.log_level = log_level
        self.log_exception = log_exception
        self.reraise = reraise
        self.operation_name = operation_name