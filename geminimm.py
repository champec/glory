import asyncio
import json
import websockets
import datetime
from websockets.legacy.server import WebSocketServerProtocol
from websockets.legacy.protocol import WebSocketCommonProtocol

# Constants
HOST = "us-central1-aiplatform.googleapis.com"
SERVICE_URL = f"wss://{HOST}/ws/google.cloud.aiplatform.v1beta1.LlmBidiService/BidiGenerateContent"

PROJECT_ID = "dogwood-reserve-427919-c0"
MODEL_NAME = f"projects/{PROJECT_ID}/locations/us-central1/publishers/google/models/gemini-2.0-flash-exp"

# Track active connections and sessions
active_connections = {}  # websocket -> connection_time
active_gemini_sessions = {}  # websocket -> session_time

async def list_active_sessions():
    """List all active sessions with their duration"""
    print("\n=== Active Sessions ===")
    print(f"Total active connections: {len(active_connections)}")
    print(f"Total Gemini sessions: {len(active_gemini_sessions)}")

    current_time = datetime.datetime.now()

    if active_connections:
        print("\nActive Connections:")
        for ws, start_time in active_connections.items():
            duration = current_time - start_time
            print(f"- Connection started at {start_time.strftime('%H:%M:%S')}, duration: {duration}")

    if active_gemini_sessions:
        print("\nGemini Sessions:")
        for ws, start_time in active_gemini_sessions.items():
            duration = current_time - start_time
            print(f"- Session started at {start_time.strftime('%H:%M:%S')}, duration: {duration}")

async def cleanup_gemini_session(server_websocket: WebSocketCommonProtocol):
    """Clean up a Gemini session"""
    try:
        if server_websocket in active_gemini_sessions:
            start_time = active_gemini_sessions[server_websocket]
            duration = datetime.datetime.now() - start_time
            print(f"\nCleaning up Gemini session started at {start_time.strftime('%H:%M:%S')} (duration: {duration})")

            try:
                cleanup_message = {
                    "contents": [{
                        "role": "user",
                        "parts": [{"text": "__cleanup_session__"}]
                    }]
                }
                await server_websocket.send(json.dumps(cleanup_message))
            except websockets.exceptions.ConnectionClosed:
                print("Connection already closed during cleanup")

            try:
                await server_websocket.close(1000, "Session cleanup")
            except Exception as e:
                print(f"Error closing websocket during cleanup: {e}")

            del active_gemini_sessions[server_websocket]
    except Exception as e:
        print(f"Error cleaning up Gemini session: {e}")

async def cleanup_all():
    """Close all active connections and sessions"""
    print("\nCleaning up existing connections and sessions...")
    await list_active_sessions()

    cleanup_tasks = []
    for session in list(active_gemini_sessions.keys()):
        cleanup_tasks.append(asyncio.create_task(cleanup_gemini_session(session)))

    for ws in list(active_connections.keys()):
        try:
            cleanup_tasks.append(asyncio.create_task(ws.close(1001, "Server restart")))
            del active_connections[ws]
        except Exception as e:
            print(f"Error closing connection: {e}")

    if cleanup_tasks:
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    active_connections.clear()
    active_gemini_sessions.clear()
    print(f"\nCleaned up {len(cleanup_tasks)} connections/sessions")

        
async def create_proxy(client_websocket, bearer_token):
    """Create a proxy to Gemini."""
    headers = {"Authorization": f"Bearer {bearer_token}"}
    async with websockets.connect(SERVICE_URL, additional_headers=headers) as server_websocket:
        active_gemini_sessions[server_websocket] = datetime.datetime.now()
        await list_active_sessions()

        setup_message = {"setup": {"model": MODEL_NAME, "generation_config": {"response_modalities": ["AUDIO"]}}}
        await server_websocket.send(json.dumps(setup_message))

        client_to_server = asyncio.create_task(proxy_task(client_websocket, server_websocket, True))
        server_to_client = asyncio.create_task(proxy_task(server_websocket, client_websocket, False))

        done, pending = await asyncio.wait([client_to_server, server_to_client], return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()

async def proxy_task(
    source_websocket: WebSocketCommonProtocol, 
    target_websocket: WebSocketCommonProtocol,
    is_client_to_server: bool = True
) -> None:
    source = "client" if is_client_to_server else "server"
    target = "server" if is_client_to_server else "client"
    
    try:
        while True:
            try:
                message = await source_websocket.recv()
                data = json.loads(message)
                print(f"\nReceived message from {source}:")
                print(json.dumps(data, indent=2))
                
                # Check for errors in server response
                if not is_client_to_server and 'error' in data:
                    print(f"\nError received from Gemini: {data['error']}")
                    if 'RESOURCE_EXHAUSTED' in data['error']:
                        print("Session limit reached, initiating cleanup...")
                        return
                
                await target_websocket.send(json.dumps(data))
                print(f"Message forwarded to {target}")
                
            except websockets.exceptions.ConnectionClosed:
                print(f"\n{source.capitalize()} connection closed")
                return
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from {source}: {e}")
                continue
            except Exception as e:
                print(f"Error in {source} proxy: {e}")
                return
    finally:
        if is_client_to_server:
            if source_websocket in active_connections:
                del active_connections[source_websocket]
            if target_websocket in active_gemini_sessions:
                await cleanup_gemini_session(target_websocket)

async def handle_client(client_websocket):
    """Handle incoming client connections."""
    active_connections[client_websocket] = datetime.datetime.now()
    try:
        auth_message = await asyncio.wait_for(client_websocket.recv(), timeout=5.0)
        auth_data = json.loads(auth_message)
        bearer_token = auth_data.get("bearer_token")
        if bearer_token:
            await create_proxy(client_websocket, bearer_token)
        else:
            await client_websocket.close(1008, "Bearer token missing")
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        if client_websocket in active_connections:  # Add this check
            del active_connections[client_websocket]


async def main():
    """Start the WebSocket server."""
    print("Starting WebSocket server...")
    await cleanup_all()
    async with websockets.serve(handle_client, "0.0.0.0", 8765):
        print("Server running on port 8765")
        await asyncio.Future()  # Keep running

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server shutdown")
