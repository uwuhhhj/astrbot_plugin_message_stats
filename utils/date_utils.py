"""
日期时间工具模块
提供日期处理和计算功能
"""

from datetime import datetime, date, timedelta
from typing import Optional
from .models import MessageDate


def get_current_date() -> MessageDate:
    """获取当前日期
    
    返回表示当前日期的MessageDate对象。
    
    Returns:
        MessageDate: 当前日期的MessageDate对象
        
    Example:
        >>> today = get_current_date()
        >>> print(today.year, today.month, today.day)
        2024 1 15
    """
    now = datetime.now()
    return MessageDate.from_datetime(now)


def get_week_start(date_obj: date) -> date:
    """获取周开始日期（周一）
    
    计算给定日期所在周的周一日期。
    
    Args:
        date_obj (date): 任意日期对象
        
    Returns:
        date: 该日期所在周的周一日期
        
    Example:
        >>> from datetime import date
        >>> d = date(2024, 1, 15)  # 周一
        >>> week_start = get_week_start(d)
        >>> print(week_start)
        2024-01-15
    """
    days_since_monday = date_obj.weekday()
    return date_obj - timedelta(days=days_since_monday)


def get_month_start(date_obj: date) -> date:
    """获取月开始日期
    
    计算给定日期所在月的月初日期（1号）。
    
    Args:
        date_obj (date): 任意日期对象
        
    Returns:
        date: 该日期所在月的月初日期
        
    Example:
        >>> from datetime import date
        >>> d = date(2024, 1, 15)
        >>> month_start = get_month_start(d)
        >>> print(month_start)
        2024-01-01
    """
    return date_obj.replace(day=1)


def is_same_week(date1: date, date2: date) -> bool:
    """判断是否是同一周
    
    比较两个日期是否在同一周内（以周一为一周的开始）。
    
    Args:
        date1 (date): 第一个日期
        date2 (date): 第二个日期
        
    Returns:
        bool: 如果在同一周返回True，否则返回False
        
    Example:
        >>> from datetime import date
        >>> d1 = date(2024, 1, 15)  # 周一
        >>> d2 = date(2024, 1, 21)  # 周日
        >>> print(is_same_week(d1, d2))
        True
    """
    return get_week_start(date1) == get_week_start(date2)


def is_same_month(date1: date, date2: date) -> bool:
    """判断是否是同一月
    
    比较两个日期是否在同一月内。
    
    Args:
        date1 (date): 第一个日期
        date2 (date): 第二个日期
        
    Returns:
        bool: 如果在同一月返回True，否则返回False
        
    Example:
        >>> from datetime import date
        >>> d1 = date(2024, 1, 15)
        >>> d2 = date(2024, 1, 25)
        >>> print(is_same_month(d1, d2))
        True
    """
    return date1.year == date2.year and date1.month == date2.month


def get_date_range_days(start_date: date, end_date: date) -> list[date]:
    """获取日期范围内的所有日期
    
    生成从开始日期到结束日期（包含）的所有日期。
    
    Args:
        start_date (date): 开始日期
        end_date (date): 结束日期
        
    Returns:
        list[date]: 日期列表
        
    Example:
        >>> from datetime import date
        >>> dates = get_date_range_days(date(2024, 1, 1), date(2024, 1, 3))
        >>> print(len(dates))
        3
    """
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    return dates