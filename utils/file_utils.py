"""
文件操作工具模块
提供异步JSON文件读写功能
"""

import json
import aiofiles
from pathlib import Path
from typing import Dict, Any


async def load_json_file(file_path: str) -> Dict[str, Any]:
    """异步加载JSON文件
    
    异步读取JSON文件并解析内容。
    
    Args:
        file_path (str): JSON文件路径
        
    Returns:
        Dict[str, Any]: 解析后的字典数据
        
    Raises:
        FileNotFoundError: 当文件不存在时
        json.JSONDecodeError: 当JSON格式错误时
        IOError: 当文件读取失败时
        OSError: 当系统操作失败时
        
    Example:
        >>> data = await load_json_file("config.json")
        >>> print(f"加载成功，包含 {len(data)} 个键")
    """
    async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
        content = await f.read()
        return json.loads(content)


async def save_json_file(file_path: str, data: Dict[str, Any]) -> None:
    """异步保存JSON文件
    
    异步将字典数据保存为JSON文件，自动创建必要的目录结构。
    
    Args:
        file_path (str): JSON文件保存路径
        data (Dict[str, Any]: 要保存的数据字典
        
    Raises:
        IOError: 当文件写入失败时
        OSError: 当文件系统操作失败时
        
    Example:
        >>> data = {"name": "测试", "value": 123}
        >>> await save_json_file("output.json", data)
        >>> print("保存完成")
    """
    # 使用pathlib创建目录，更现代且简洁
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    
    async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))