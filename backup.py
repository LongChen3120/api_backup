import sys
import json
import datetime

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

sys.path.append("../src")
import _env
from load import load_file


# Kết nối tới Redis
redis = Redis(host="localhost", port=6379, encoding="utf-8", decode_responses=True)

# Tạo engine không đồng bộ, max pool 100 để duy trì kết nối sẵn tới mysql
engine = create_async_engine(
    _env.CONNECTION_STRINGG,
    pool_size=100,
    max_overflow=50,
    pool_pre_ping=True,
)

# Tạo session factory không đồng bộ
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

app = FastAPI()

# Lấy session không đồng bộ
async def get_db_connection():
    async with AsyncSessionLocal() as session:
        return session

# Lấy cache từ Redis
async def get_cache(key):
    return await redis.get(key)

# Lưu cache vào Redis
async def set_cache(key, value, expire=60):
    await redis.set(key, value, ex=expire)

@app.get("/get_data")
async def get_data_ps(token_id: str, day: int):
    try:
        start_time = datetime.datetime.now()
        cache_key = f"get_data_{token_id}_{day}"

        # Kiểm tra cache
        cached_data = await get_cache(cache_key)
        if cached_data:
            data = json.loads(cached_data) # data lưu trong cache dạng json, chuyển thành list
            end_time = datetime.datetime.now()
            return {
                "message":"Request ok",
                "code": 200,
                "time":(end_time - start_time).total_seconds() * 1000,
                "data":data
            }
        # Nếu không có cache thì truy vấn MySQL
        async with await get_db_connection() as session:
            date_from_day = datetime.datetime.now().date() - datetime.timedelta(days=day)
            query = text(f"""
                SELECT time_point, mid_point, last_trade_price
                FROM details_event
                WHERE id_token = :token_id AND time_point > :date_from_day
            """)
            result = await session.execute(query, {"token_id": token_id, "date_from_day": date_from_day})
            data = result.fetchall()

            temp = [{
                "time": item[0].isoformat(),
                "mid_point": item[1],
                "last_trade_price": item[2]
            } for item in data]

            # Lưu kết quả vào Redis Cache
            value = json.dumps(temp)
            await set_cache(cache_key, value)
        end_time = datetime.datetime.now()
        return {
                "message":"Request ok",
                "code": 200,
                "time":(end_time - start_time).total_seconds() * 1000,
                "data":temp
            }
    except Exception as e:
        print(e)
        end_time = datetime.datetime.now()
        return {
            "message":"Internal Server Error",
            "code": 500,
            "time": (end_time - start_time).total_seconds() * 1000,
            "data": None
        }
