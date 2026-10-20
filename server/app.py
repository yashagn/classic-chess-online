import asyncio
import json
import os
import secrets
import string
from dataclasses import dataclass, field

import chess
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()
rooms = {}
ALPHABET = string.ascii_uppercase + string.digits


@dataclass
class Room:
    board: chess.Board = field(default_factory=chess.Board)
    players: dict = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def state(room, color):
    return {"type": "state", "fen": room.board.fen(), "moves": [m.uci() for m in room.board.move_stack],
            "color": color, "ready": len(room.players) == 2,
            "game_over": room.board.is_game_over(claim_draw=True)}


async def send(ws, data):
    await ws.send_text(json.dumps(data))


async def broadcast(room):
    for color, ws in list(room.players.items()):
        try:
            await send(ws, state(room, color))
        except Exception:
            pass


@app.get("/")
def health():
    return {"status": "ok", "service": "Classic Chess Online"}


@app.websocket("/ws")
async def game(ws: WebSocket):
    await ws.accept()
    code = None
    color = None
    room = None
    try:
        first = json.loads(await ws.receive_text())
        action = first.get("action")
        if action == "create":
            code = ''.join(secrets.choice(ALPHABET) for _ in range(6))
            while code in rooms:
                code = ''.join(secrets.choice(ALPHABET) for _ in range(6))
            room = Room()
            rooms[code] = room
            color = "white"
            room.players[color] = ws
        elif action == "join":
            code = str(first.get("code", "")).strip().upper()
            room = rooms.get(code)
            if not room or len(room.players) != 1 or "black" in room.players:
                await send(ws, {"type": "error", "message": "Room not found or full"})
                return
            color = "black"
            room.players[color] = ws
        else:
            await send(ws, {"type": "error", "message": "Invalid action"})
            return
        await send(ws, {"type": "welcome", "code": code, "color": color})
        await broadcast(room)
        while True:
            msg = json.loads(await ws.receive_text())
            if msg.get("action") != "move":
                continue
            async with room.lock:
                if len(room.players) != 2:
                    await send(ws, {"type": "error", "message": "Waiting for opponent"})
                    continue
                if room.board.is_game_over(claim_draw=True):
                    await send(ws, {"type": "error", "message": "Game finished"})
                    continue
                if (room.board.turn == chess.WHITE) != (color == "white"):
                    await send(ws, {"type": "error", "message": "Not your turn"})
                    continue
                try:
                    move = chess.Move.from_uci(str(msg.get("uci", "")))
                except ValueError:
                    move = None
                if move not in room.board.legal_moves:
                    await send(ws, {"type": "error", "message": "Illegal move"})
                    continue
                room.board.push(move)
                await broadcast(room)
    except (WebSocketDisconnect, json.JSONDecodeError):
        pass
    finally:
        if room and color and room.players.get(color) is ws:
            room.players.pop(color, None)
            await broadcast(room)
            if not room.players:
                rooms.pop(code, None)
