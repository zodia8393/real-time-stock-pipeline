from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import psycopg2
import redis # type: ignore
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_client import CONTENT_TYPE_LATEST

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI 앱 생성
app = FastAPI(
    title="Real-time Stock Pipeline API",
    description="실시간 주식 데이터 파이프라인 대시보드",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus 메트릭
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint'])
REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration')

# 데이터베이스 연결
def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        database="stock_data", 
        user="admin",
        password="password123",
        port="5432"
    )

def get_redis_connection():
    return redis.Redis(
        host='localhost',
        port=6379,
        decode_responses=True
    )

@app.middleware("http")
async def add_process_time_header(request, call_next):
    """요청 처리 시간 측정"""
    with REQUEST_DURATION.time():
        REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path).inc()
        response = await call_next(request)
        return response

# ==================== API 엔드포인트 ====================

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """메인 대시보드 페이지"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>📊 실시간 주식 데이터 파이프라인</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .stock-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .stock-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .positive { color: #22c55e; } .negative { color: #ef4444; }
            .price { font-size: 24px; font-weight: bold; }
            .change { font-size: 14px; }
            h1 { color: #333; text-align: center; }
            .api-links { display: flex; gap: 10px; flex-wrap: wrap; justify-content: center; }
            .api-link { background: #3b82f6; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; }
            .api-link:hover { background: #2563eb; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 실시간 주식 데이터 파이프라인 대시보드</h1>
            
            <div class="card">
                <h2>🔗 API 엔드포인트</h2>
                <div class="api-links">
                    <a href="/stocks/current" class="api-link">📈 실시간 주가</a>
                    <a href="/stocks/history/AAPL?hours=1" class="api-link">📊 AAPL 히스토리</a>
                    <a href="/stats" class="api-link">📋 시스템 통계</a>
                    <a href="/health" class="api-link">💚 헬스체크</a>
                    <a href="/metrics" class="api-link">📊 메트릭</a>
                    <a href="/docs" class="api-link">📚 API 문서</a>
                </div>
            </div>

            <div class="card">
                <h2>🎯 프로젝트 특징</h2>
                <ul>
                    <li>✅ <strong>Apache Kafka</strong>를 통한 실시간 데이터 스트리밍</li>
                    <li>✅ <strong>PostgreSQL + InfluxDB + Redis</strong> 멀티 데이터베이스</li>
                    <li>✅ <strong>Docker Compose</strong> 마이크로서비스 아키텍처</li>
                    <li>✅ <strong>FastAPI</strong> REST API 서버</li>
                    <li>✅ <strong>Prometheus + Grafana</strong> 모니터링</li>
                    <li>✅ <strong>내결함성</strong> 장애 대응 설계</li>
                </ul>
            </div>
        </div>
    </body>
    </html>
    """
    return html_content

@app.get("/stocks/current")
async def get_current_stocks():
    """Redis에서 모든 주식의 최신 가격 조회"""
    try:
        redis_client = get_redis_connection()
        stocks = ['AAPL', 'GOOGL', 'TSLA', 'NVDA', 'MSFT', 'AMZN']
        result = {}
        
        for symbol in stocks:
            stock_data = redis_client.hgetall(f"stock:{symbol}")
            if stock_data:
                result[symbol] = {
                    'symbol': symbol,
                    'price': float(stock_data.get('price', 0)),
                    'change_percent': float(stock_data.get('change_percent', 0)),
                    'timestamp': stock_data.get('timestamp'),
                    'status': 'live'
                }
        
        return {
            'status': 'success',
            'data': result,
            'total_stocks': len(result),
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting current stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{symbol}")
async def get_stock_detail(symbol: str):
    """특정 주식의 상세 정보 조회"""
    try:
        # Redis에서 최신 데이터
        redis_client = get_redis_connection()
        current_data = redis_client.hgetall(f"stock:{symbol.upper()}")
        
        # PostgreSQL에서 최근 데이터
        conn = get_postgres_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT symbol, price, change_percent, volume, timestamp
                FROM stock_prices 
                WHERE symbol = %s 
                ORDER BY timestamp DESC 
                LIMIT 10
            """, (symbol.upper(),))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'symbol': row[0],
                    'price': float(row[1]),
                    'change_percent': float(row[2]),
                    'volume': int(row[3]),
                    'timestamp': row[4].isoformat()
                })
        
        conn.close()
        
        if not current_data and not history:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        
        return {
            'symbol': symbol.upper(),
            'current': {
                'price': float(current_data.get('price', 0)) if current_data else 0,
                'change_percent': float(current_data.get('change_percent', 0)) if current_data else 0,
                'timestamp': current_data.get('timestamp') if current_data else None
            },
            'recent_history': history[:5],
            'total_records': len(history)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stock detail for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/history/{symbol}")
async def get_stock_history(symbol: str, hours: int = 24):
    """주식의 시간별 히스토리 조회"""
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT symbol, price, change_percent, volume, timestamp
                FROM stock_prices 
                WHERE symbol = %s 
                AND timestamp >= NOW() - INTERVAL '%s hours'
                ORDER BY timestamp DESC
            """, (symbol.upper(), hours))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'symbol': row[0],
                    'price': float(row[1]),
                    'change_percent': float(row[2]),
                    'volume': int(row[3]),
                    'timestamp': row[4].isoformat()
                })
        
        conn.close()
        
        return {
            'symbol': symbol.upper(),
            'period_hours': hours,
            'data': history,
            'total_records': len(history)
        }
        
    except Exception as e:
        logger.error(f"Error getting history for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_system_stats():
    """시스템 통계 조회"""
    try:
        conn = get_postgres_connection()
        redis_client = get_redis_connection()
        
        with conn.cursor() as cursor:
            # 총 레코드 수
            cursor.execute("SELECT COUNT(*) FROM stock_prices")
            total_records = cursor.fetchone()[0]
            
            # 오늘 레코드 수
            cursor.execute("""
                SELECT COUNT(*) FROM stock_prices 
                WHERE DATE(timestamp) = CURRENT_DATE
            """)
            today_records = cursor.fetchone()[0]
            
            # 주식별 통계
            cursor.execute("""
                SELECT symbol, COUNT(*), AVG(price), MIN(timestamp), MAX(timestamp)
                FROM stock_prices 
                GROUP BY symbol 
                ORDER BY COUNT(*) DESC
            """)
            
            symbol_stats = []
            for row in cursor.fetchall():
                symbol_stats.append({
                    'symbol': row[0],
                    'total_records': int(row[1]),
                    'avg_price': round(float(row[2]), 2),
                    'first_record': row[3].isoformat(),
                    'last_record': row[4].isoformat()
                })
        
        conn.close()
        
        # Redis 통계
        redis_info = redis_client.info()
        
        return {
            'database_stats': {
                'total_records': total_records,
                'today_records': today_records,
                'symbol_breakdown': symbol_stats
            },
            'redis_stats': {
                'connected_clients': redis_info.get('connected_clients', 0),
                'used_memory': redis_info.get('used_memory_human', '0B'),
                'total_commands_processed': redis_info.get('total_commands_processed', 0)
            },
            'system_status': 'healthy',
            'last_updated': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting system stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """시스템 헬스체크"""
    try:
        # PostgreSQL 연결 테스트
        conn = get_postgres_connection()
        conn.close()
        postgres_status = "healthy"
    except:
        postgres_status = "unhealthy"
    
    try:
        # Redis 연결 테스트
        redis_client = get_redis_connection()
        redis_client.ping()
        redis_status = "healthy"
    except:
        redis_status = "unhealthy"
    
    overall_status = "healthy" if postgres_status == redis_status == "healthy" else "degraded"
    
    return {
        'status': overall_status,
        'services': {
            'postgres': postgres_status,
            'redis': redis_status,
            'api': 'healthy'
        },
        'timestamp': datetime.now().isoformat()
    }

@app.get("/metrics")
async def get_metrics():
    """Prometheus 메트릭 엔드포인트"""
    return generate_latest()


@app.get("/stocks/chart/{symbol}")
async def get_stock_chart_data(symbol: str, period: str = "1d"):
    """차트용 전체 주식 데이터 조회"""
    try:
        conn = get_postgres_connection()
        
        # 기간별 쿼리
        time_filters = {
            "1h": "1 hour",
            "1d": "1 day", 
            "3d": "3 days",
            "1w": "7 days",
            "1m": "30 days",
            "all": None
        }
        
        with conn.cursor() as cursor:
            if period == "all":
                cursor.execute("""
                    SELECT symbol, price, volume, change_percent, timestamp
                    FROM stock_prices 
                    WHERE symbol = %s 
                    ORDER BY timestamp ASC
                """, (symbol.upper(),))
            else:
                time_period = time_filters.get(period, "1 day")
                cursor.execute("""
                    SELECT symbol, price, volume, change_percent, timestamp
                    FROM stock_prices 
                    WHERE symbol = %s 
                    AND timestamp >= NOW() - INTERVAL %s
                    ORDER BY timestamp ASC
                """, (symbol.upper(), time_period))
            
            data = []
            for row in cursor.fetchall():
                data.append({
                    'symbol': row[0],
                    'price': float(row[1]),
                    'volume': int(row[2]),
                    'change_percent': float(row[3]),
                    'timestamp': row[4].isoformat(),
                    'date': row[4].strftime('%Y-%m-%d %H:%M:%S')
                })
        
        conn.close()
        
        return {
            'symbol': symbol.upper(),
            'period': period,
            'total_records': len(data),
            'data': data
        }
        
    except Exception as e:
        logger.error(f"Error getting chart data for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/all-symbols")
async def get_all_symbols():
    """저장된 모든 주식 심볼 조회"""
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT symbol, COUNT(*)
                FROM stock_prices 
                GROUP BY symbol 
                ORDER BY COUNT(*) DESC
            """)
            
            symbols = []
            for row in cursor.fetchall():
                symbols.append({
                    'symbol': row[0],
                    'records': int(row[1])
                })
        
        conn.close()
        return {'symbols': symbols}
        
    except Exception as e:
        logger.error(f"Error getting symbols: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/charts", response_class=HTMLResponse)
async def charts_dashboard():
    """실시간 차트 대시보드"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>📈 실시간 주식 차트 대시보드</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 1400px; margin: 0 auto; }
            .controls { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .chart-container { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; height: 500px; }
            select, button { padding: 10px; margin: 5px; border-radius: 4px; border: 1px solid #ddd; }
            button { background: #3b82f6; color: white; cursor: pointer; }
            button:hover { background: #2563eb; }
            .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
            .symbol-card { background: white; padding: 15px; border-radius: 8px; cursor: pointer; }
            .symbol-card:hover { background: #f8f9fa; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📈 실시간 주식 차트 대시보드</h1>
            
            <div class="controls">
                <select id="symbolSelect">
                    <option value="">종목 선택...</option>
                </select>
                
                <select id="periodSelect">
                    <option value="1h">1시간</option>
                    <option value="1d" selected>1일</option>
                    <option value="3d">3일</option>
                    <option value="1w">1주일</option>
                    <option value="1m">1개월</option>
                    <option value="all">전체</option>
                </select>
                
                <button onclick="loadChart()">차트 업데이트</button>
                <button onclick="autoRefresh()">자동 새로고침 ON/OFF</button>
            </div>

            <div class="grid">
                <div class="chart-container">
                    <canvas id="priceChart"></canvas>
                </div>
                <div class="chart-container">
                    <canvas id="volumeChart"></canvas>
                </div>
            </div>
            
            <div id="symbolsList" class="grid"></div>
        </div>

        <script>
            let priceChart, volumeChart;
            let autoRefreshInterval;
            let isAutoRefresh = false;
            
            // 차트 초기화
            function initCharts() {
                const priceCtx = document.getElementById('priceChart').getContext('2d');
                const volumeCtx = document.getElementById('volumeChart').getContext('2d');
                
                priceChart = new Chart(priceCtx, {
                    type: 'line',
                    data: {
                        labels: [],
                        datasets: [{
                            label: '주가 (₩)',
                            data: [],
                            borderColor: '#3b82f6',
                            backgroundColor: 'rgba(59, 130, 246, 0.1)',
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: {
                                beginAtZero: false,
                                ticks: {
                                    callback: function(value) {
                                        return '₩' + value.toLocaleString();
                                    }
                                }
                            }
                        }
                    }
                });
                
                volumeChart = new Chart(volumeCtx, {
                    type: 'bar',
                    data: {
                        labels: [],
                        datasets: [{
                            label: '거래량',
                            data: [],
                            backgroundColor: 'rgba(34, 197, 94, 0.7)',
                            borderColor: '#22c55e'
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: {
                                beginAtZero: true,
                                ticks: {
                                    callback: function(value) {
                                        return value.toLocaleString();
                                    }
                                }
                            }
                        }
                    }
                });
            }
            
            // 주식 심볼 목록 로드
            async function loadSymbols() {
                try {
                    const response = await fetch('/stocks/all-symbols');
                    const data = await response.json();
                    
                    const select = document.getElementById('symbolSelect');
                    const symbolsList = document.getElementById('symbolsList');
                    
                    select.innerHTML = '<option value="">종목 선택...</option>';
                    symbolsList.innerHTML = '';
                    
                    data.symbols.forEach(item => {
                        // 드롭다운 옵션 추가
                        const option = document.createElement('option');
                        option.value = item.symbol;
                        option.textContent = `${item.symbol} (${item.records}개 기록)`;
                        select.appendChild(option);
                        
                        // 심볼 카드 추가
                        const card = document.createElement('div');
                        card.className = 'symbol-card';
                        card.innerHTML = `
                            <h3>${item.symbol}</h3>
                            <p>총 ${item.records}개 기록</p>
                        `;
                        card.onclick = () => {
                            select.value = item.symbol;
                            loadChart();
                        };
                        symbolsList.appendChild(card);
                    });
                } catch (error) {
                    console.error('Error loading symbols:', error);
                }
            }
            
            // 차트 데이터 로드
            async function loadChart() {
                const symbol = document.getElementById('symbolSelect').value;
                const period = document.getElementById('periodSelect').value;
                
                if (!symbol) {
                    alert('종목을 선택해주세요!');
                    return;
                }
                
                try {
                    const response = await fetch(`/stocks/chart/${symbol}?period=${period}`);
                    const data = await response.json();
                    
                    if (data.data.length === 0) {
                        alert('해당 기간에 데이터가 없습니다!');
                        return;
                    }
                    
                    const labels = data.data.map(item => {
                        const date = new Date(item.timestamp);
                        return date.toLocaleString('ko-KR');
                    });
                    
                    const prices = data.data.map(item => item.price);
                    const volumes = data.data.map(item => item.volume);
                    
                    // 가격 차트 업데이트
                    priceChart.data.labels = labels;
                    priceChart.data.datasets[0].data = prices;
                    priceChart.data.datasets[0].label = `${symbol} 주가 (₩)`;
                    priceChart.update();
                    
                    // 거래량 차트 업데이트
                    volumeChart.data.labels = labels;
                    volumeChart.data.datasets[0].data = volumes;
                    volumeChart.data.datasets[0].label = `${symbol} 거래량`;
                    volumeChart.update();
                    
                    console.log(`차트 업데이트 완료: ${symbol} (${data.total_records}개 데이터)`);
                    
                } catch (error) {
                    console.error('Error loading chart:', error);
                    alert('차트 로드 중 오류가 발생했습니다!');
                }
            }
            
            // 자동 새로고침
            function autoRefresh() {
                if (isAutoRefresh) {
                    clearInterval(autoRefreshInterval);
                    isAutoRefresh = false;
                    console.log('자동 새로고침 OFF');
                } else {
                    autoRefreshInterval = setInterval(loadChart, 30000); // 30초마다
                    isAutoRefresh = true;
                    console.log('자동 새로고침 ON (30초마다)');
                }
            }
            
            // 페이지 로드시 초기화
            window.onload = function() {
                initCharts();
                loadSymbols();
            };
        </script>
    </body>
    </html>
    """
    return html_content



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
