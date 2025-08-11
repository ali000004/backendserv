#!/usr/bin/env python3
"""
FastAPI Server for Trading Bot Dashboard
Serves real-time data and controls for the BTC Shadow Trading Bot
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import json
import os
import csv
import glob
from datetime import datetime, timedelta
from typing import Dict, List, Set
from pydantic import BaseModel
import uvicorn

import math

# Global variables
connected_clients: Set[WebSocket] = set()
bot_state = {
    "status": "disconnected",
    "current_price": 0,
    "active_position": None,
    "parameters": {},
    "recent_trades": [],
    "order_flow": {},
    "performance_metrics": {}
}

# === Utility Functions ===
def clean_float_values(obj):
    """Recursively clean infinity and NaN values from data structures"""
    if isinstance(obj, dict):
        return {k: clean_float_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_float_values(item) for item in obj]
    elif isinstance(obj, float):
        if math.isinf(obj) or math.isnan(obj):
            return 1.0  # Default safe value
        return obj
    else:
        return obj

# === Data Loading Functions ===
def load_recent_trades_from_logs(limit: int = 50):
    """Load recent trades from CSV log files"""
    log_dir = "C:/Users/Administrator/Desktop/btclogs"
    trades = []
    
    # Find most recent trade files
    pattern = os.path.join(log_dir, "**", "*trades*.csv")
    files = glob.glob(pattern, recursive=True)
    
    for file_path in sorted(files, reverse=True):
        try:
            with open(file_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('entry_price') and row.get('exit_price'):
                        trade = {
                            'timestamp': row['timestamp'],
                            'direction': row['direction'],
                            'entry_price': float(row['entry_price']),
                            'exit_price': float(row['exit_price']),
                            'result': row['result'],
                            'duration_sec': float(row.get('duration_sec', 0))
                        }
                        
                        # Calculate P&L
                        if trade['direction'] == 'long':
                            trade['pnl_pct'] = (trade['exit_price'] - trade['entry_price']) / trade['entry_price'] * 100
                        else:
                            trade['pnl_pct'] = (trade['entry_price'] - trade['exit_price']) / trade['entry_price'] * 100
                        
                        trades.append(trade)
                        
                        if len(trades) >= limit:
                            return trades[:limit]
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
    
    return trades

def calculate_performance_from_logs():
    """Calculate performance metrics from log files"""
    trades = load_recent_trades_from_logs(1000)  # Get more data for analysis
    
    if not trades:
        return {"error": "No trades found"}
    
    total_trades = len(trades)
    winning_trades = [t for t in trades if t['pnl_pct'] > 0]
    
    win_count = len(winning_trades)
    win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
    
    total_pnl = sum(t['pnl_pct'] for t in trades)
    avg_win = sum(t['pnl_pct'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
    avg_loss = sum(t['pnl_pct'] for t in trades if t['pnl_pct'] <= 0) / (total_trades - win_count) if (total_trades - win_count) > 0 else 0
    
    return {
        "total_trades": total_trades,
        "win_count": win_count,
        "loss_count": total_trades - win_count,
        "win_rate": round(win_rate, 2),
        "total_pnl": round(total_pnl, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2)
    }

def load_logs_by_type(log_type: str, hours: int = 24):
    """Load specific log type (diagnostics, opportunities, etc.)"""
    log_dir = "C:/Users/Administrator/Desktop/btclogs"
    logs = []
    
    pattern = os.path.join(log_dir, "**", f"*{log_type}*.csv")
    files = glob.glob(pattern, recursive=True)
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    
    for file_path in sorted(files, reverse=True)[:5]:  # Latest 5 files
        try:
            with open(file_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        timestamp = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                        if timestamp >= cutoff_time:
                            logs.append(dict(row))
                    except:
                        logs.append(dict(row))  # Include if timestamp parsing fails
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
    
    return logs[-500:]  # Return latest 500 entries

# === Real-time Data Simulation ===
async def simulate_real_time_data():
    """Placeholder function - real data only, no simulation"""
    # This function now does nothing - we only want real market data
    while True:
        try:
            await asyncio.sleep(60)  # Just keep the task alive but do nothing
        except asyncio.CancelledError:
            break

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Trading Bot Dashboard API starting up...")
    print("Real market data only - no simulation")
    
    # Initialize bot parameters
    bot_state["parameters"] = {
        "CAPITAL": 20000,
        "TP_TARGET": 0.009,
        "SL1_PCT": 0.012,
        "SL2_TRIGGER_PCT": 0.005,
        "SL2_PCT": 0.004,
        "FLOW_IMBALANCE_THRESHOLD": 1.3
    }
    
    # Initialize with default values (will be overwritten by real data)
    bot_state["current_price"] = 0
    bot_state["order_flow"] = {"buys": 0, "sells": 0, "imbalance_ratio": 1.0}
    
    # Start placeholder task (does nothing now)
    task = asyncio.create_task(simulate_real_time_data())
    
    yield
    
    # Shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    print("Trading Bot Dashboard API shutting down...")

app = FastAPI(title="Trading Bot Dashboard", lifespan=lifespan)

# CORS middleware for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],  # Explicitly allow OPTIONS
    allow_headers=["*"],
)

# Configuration models
class BotParameters(BaseModel):
    CAPITAL: float
    TP_TARGET: float
    SL1_PCT: float
    SL2_TRIGGER_PCT: float
    SL2_PCT: float
    FLOW_IMBALANCE_THRESHOLD: float

class BotCommand(BaseModel):
    action: str  # "start", "stop", "update_params"
    parameters: Dict = {}

# === WebSocket Manager ===
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except:
                disconnected.append(connection)
        
        # Remove disconnected clients
        for conn in disconnected:
            if conn in self.active_connections:
                self.active_connections.remove(conn)

manager = ConnectionManager()

# === Root endpoint ===
@app.get("/")
async def read_root():
    """Root endpoint - API info"""
    return {
        "message": "Trading Bot Dashboard API",
        "status": "running",
        "endpoints": {
            "status": "/api/status",
            "trades": "/api/trades",
            "performance": "/api/performance",
            "control": "/api/control",
            "websocket": "/ws"
        }
    }

# === API Routes ===
@app.get("/api/status")
async def get_bot_status():
    """Get current bot status and metrics"""
    # Clean the bot state before returning to avoid infinity values
    cleaned_state = clean_float_values(bot_state)
    return cleaned_state

@app.post("/api/control")
async def control_bot(command: BotCommand):
    """Send commands to the bot"""
    # Here you would implement bot control logic
    # For now, just update the state
    if command.action == "update_params":
        bot_state["parameters"].update(command.parameters)
        await manager.broadcast({
            "type": "parameters_updated",
            "data": bot_state["parameters"]
        })
        return {"status": "success", "message": "Parameters updated"}
    
    elif command.action == "start":
        bot_state["status"] = "running"
        await manager.broadcast({
            "type": "status_change",
            "data": {"status": "running"}
        })
        return {"status": "success", "message": "Bot started"}
    
    elif command.action == "stop":
        bot_state["status"] = "stopped"
        await manager.broadcast({
            "type": "status_change",
            "data": {"status": "stopped"}
        })
        return {"status": "success", "message": "Bot stopped"}
    
    else:
        raise HTTPException(status_code=400, detail="Invalid command")

@app.get("/api/trades")
async def get_recent_trades(limit: int = 50):
    """Get recent trades from log files"""
    try:
        trades = load_recent_trades_from_logs(limit)
        return {"trades": trades}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/performance")
async def get_performance_metrics():
    """Get performance analysis"""
    try:
        metrics = calculate_performance_from_logs()
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/real_time_update")
async def receive_real_time_update(data: dict):
    """Receive real-time updates from the bot"""
    try:
        # Clean the incoming data to remove any infinity values
        cleaned_data = clean_float_values(data)
        
        # Update bot state with received data
        bot_state["current_price"] = cleaned_data.get("price", bot_state["current_price"])
        bot_state["order_flow"] = cleaned_data.get("order_flow", bot_state["order_flow"])
        bot_state["active_position"] = cleaned_data.get("active_position", bot_state["active_position"])
        
        # Broadcast to all connected WebSocket clients
        await manager.broadcast({
            "type": "real_time_update",
            "data": {
                "price": cleaned_data.get("price", 0),
                "order_flow": cleaned_data.get("order_flow", {}),
                "active_position": cleaned_data.get("active_position"),
                "timestamp": cleaned_data.get("timestamp", datetime.now().isoformat())
            }
        })
        
        return {"status": "success", "message": "Data received and broadcasted"}
    except Exception as e:
        print(f"Error processing real-time update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trade_completed")
async def receive_trade_completed(trade_data: dict):
    """Receive trade completion notifications from the bot"""
    try:
        # Add to recent trades
        if "recent_trades" not in bot_state:
            bot_state["recent_trades"] = []
        
        bot_state["recent_trades"].insert(0, trade_data)
        bot_state["recent_trades"] = bot_state["recent_trades"][:50]  # Keep last 50 trades
        
        # Clear active position
        bot_state["active_position"] = None
        
        # Broadcast trade completion
        await manager.broadcast({
            "type": "trade_completed",
            "data": trade_data
        })
        
        return {"status": "success", "message": "Trade completion received"}
    except Exception as e:
        print(f"Error processing trade completion: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/logs/{log_type}")
async def get_logs(log_type: str, hours: int = 24):
    """Get specific log data (diagnostics, opportunities, etc.)"""
    try:
        logs = load_logs_by_type(log_type, hours)
        return {"logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === WebSocket Endpoint ===
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial state
        await websocket.send_text(json.dumps({
            "type": "initial_state",
            "data": bot_state
        }))
        
        # Keep connection alive and listen for client messages
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            # Handle client requests
            if message.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    # For HTTPS (to work with v0 Vercel frontend)
    # uvicorn.run("dashboard_server:app", host="0.0.0.0", port=8000, reload=True, ssl_keyfile="key.pem", ssl_certfile="cert.pem")
    
    # For HTTP (local development only)
    uvicorn.run("dashboard_server:app", host="0.0.0.0", port=8000, reload=True)