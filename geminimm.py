import asyncio
import json
import websockets
import datetime
import ssl
import aiohttp
from websockets.legacy.server import WebSocketServerProtocol
from websockets.legacy.protocol import WebSocketCommonProtocol

# Constants
HOST = "us-central1-aiplatform.googleapis.com"
SERVICE_URL = f"wss://{HOST}/ws/google.cloud.aiplatform.v1beta1.LlmBidiService/BidiGenerateContent"
PROJECT_ID = "dogwood-reserve-427919-c0"
MODEL_NAME = f"projects/{PROJECT_ID}/locations/us-central1/publishers/google/models/gemini-2.0-flash-exp"
SUPABASE_ENDPOINT = "https://vxroqeomlzwpuwgrrspk.supabase.co/functions/v1/execute_sql"

active_connections = {}  # websocket -> connection_time
active_gemini_sessions = {}  # websocket -> session_time


def execute_sql_query(sql_query: str, user_id: str) -> dict:
    """
    Execute a SQL query to fetch or modify todo items for the current user.

    Args:
        sql_query (str): The SQL query to execute. Must be a SELECT, INSERT, or UPDATE statement and include user_id in WHERE clause.
        user_id (str): The user ID to filter todos by.

    Returns:
        dict: The result of the SQL query execution.
    """
    return set_light_values(sql_query, user_id)  # Replace with your actual implementation


# Function declarations for Gemini
FUNCTION_DECLARATIONS = {
    "tools": [{
        "functionDeclarations": [{
            "name": "execute_sql_query",
            "description": "Execute a SQL query to fetch or modify todo items for the current user",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "sql_query": {
                        "type": "STRING",
                        "description": "The SQL query to execute. Must be a SELECT, INSERT, or UPDATE statement and include user_id in WHERE clause"
                    },
                    "user_id": {
                        "type": "STRING",
                        "description": "The user ID to filter todos by"
                    }
                },
                "required": ["sql_query", "user_id"]
            }
        }]
    }]
}

async def fetch_edge_function(args):
    """
    Makes HTTP request to Supabase Edge Function endpoint to execute SQL queries
    """
    async with aiohttp.ClientSession() as session:
        try:
            payload = {
                "sql_query": args["sql_query"],
                "user_id": args["user_id"]
            }
            headers = {
                "Content-Type": "application/json"
            }
            
            async with session.post(
                SUPABASE_ENDPOINT,
                json=payload,
                headers=headers
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"Edge function error (status {response.status}): {error_text}")
                    
        except aiohttp.ClientError as e:
            raise Exception(f"Network error calling edge function: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response from edge function: {str(e)}")
        except Exception as e:
            raise Exception(f"Error executing SQL query: {str(e)}")

async def execute_function(function_call):
    """Execute the function called by Gemini"""
    if function_call["name"] == "execute_sql_query":
        try:
            response = await fetch_edge_function(function_call["args"])
            return {
                "name": function_call["name"],
                "response": response
            }
        except Exception as e:
            return {
                "name": function_call["name"],
                "error": str(e)
            }
    return None

async def proxy_task(
    source_websocket: WebSocketCommonProtocol, 
    target_websocket: WebSocketCommonProtocol,
    user_id: str,
    is_client_to_server: bool = True
) -> None:
    try:
        source = "client" if is_client_to_server else "server"
        target = "server" if is_client_to_server else "client"
        
        while True:
            try:
                message = await source_websocket.recv()
                data = json.loads(message)
                print(f"\nReceived message from {source}:")
                print(json.dumps(data, indent=2))
                
                if not is_client_to_server:
                    if "functionCall" in data:
                        function_call = data["functionCall"]
                        function_call["args"]["user_id"] = user_id
                        function_response = await execute_function(function_call)
                        if function_response:
                            await target_websocket.send(json.dumps({
                                "functionResponse": function_response
                            }))
                            continue
                    
                    if 'error' in data:
                        print(f"\nError received from Gemini: {data['error']}")
                        if 'RESOURCE_EXHAUSTED' in data['error']:
                            print("Session limit reached, closing connection...")
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
    except Exception as e:
        print(f"Error in proxy task: {e}")
        raise

async def create_proxy(client_websocket: WebSocketCommonProtocol, bearer_token: str, auth_data: dict):
    """Creates a proxy connection between the client and Gemini service."""
    headers = {"Authorization": f"Bearer {bearer_token}"}
    server_websocket = None

    try:
        async with websockets.connect(SERVICE_URL, additional_headers=headers) as server_ws:
            server_websocket = server_ws
            print("Connected to Gemini service")
            active_gemini_sessions[server_websocket] = datetime.datetime.now()

            # Extract data from auth message
            base_system_prompt = auth_data.get('system_prompt', '')
            user_id = auth_data.get('user_id')
         

            if not user_id:
                raise ValueError("User ID is missing in auth data.")

            full_system_prompt = f"""{base_system_prompt}


Current user_id: {user_id}

Remember: Always include user_id in WHERE clauses for data security."""

            setup_message = {
            "setup": {
                "model": MODEL_NAME,
                "generation_config": {
                    "response_modalities": ["AUDIO"],
                },
                # "tools": FUNCTION_DECLARATIONS["tools"],
        #         "function_calling_config": {  # Add this block
        #     "mode": "AUTO"  # or "ANY" based on your requirement
        #     # "allowed_function_names": ["execute_sql_query"]  # Optional
        # },
                # "system_instruction": {
                #     "parts": [{
                #         "text": full_system_prompt
                #     }]
                # }
            }
        }
            
            print("Sending setup message with system prompt and schema...")
            await server_websocket.send(json.dumps(setup_message))
            print("Setup message sent to Gemini")
            
            try:
                setup_response = await asyncio.wait_for(server_websocket.recv(), timeout=5.0)
                print("Received setup response:", setup_response)
                setup_data = json.loads(setup_response)
                if 'error' in setup_data:
                    raise Exception(f"Setup error: {setup_data['error']}")
            except asyncio.TimeoutError:
                print("Timeout waiting for setup response")
                raise
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in setup response: {e}")
                raise
            except Exception as e:
                print(f"Error during setup: {e}")
                raise

            print("Setup complete, starting proxy tasks")
            client_to_server = asyncio.create_task(
                proxy_task(client_websocket, server_websocket, user_id, is_client_to_server=True)
            )
            server_to_client = asyncio.create_task(
                proxy_task(server_websocket, client_websocket, user_id, is_client_to_server=False)
            )

            try:
                done, pending = await asyncio.wait(
                    [client_to_server, server_to_client],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in done:
                    if task.exception():
                        raise task.exception()
                
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            except Exception as e:
                print(f"Error in proxy tasks: {e}")
                raise

    except websockets.exceptions.ConnectionClosed as e:
        print(f"WebSocket connection closed: code={e.code}, reason={e.reason}")
        raise
    except Exception as e:
        print(f"Error in proxy connection: {type(e).__name__}: {str(e)}")
        raise
    finally:
        if server_websocket and server_websocket in active_gemini_sessions:
            del active_gemini_sessions[server_websocket]
        print("Proxy connection closed and cleaned up.")

async def handle_client(websocket):
    print(f"New connection from {websocket.remote_address}")
    active_connections[websocket] = datetime.datetime.now()
    
    try:
        auth_message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
        auth_data = json.loads(auth_message)
        bearer_token = auth_data.get("bearer_token")
        
        if bearer_token:
            print("Authentication received, creating proxy")
            await create_proxy(websocket, bearer_token, auth_data)
        else:
            print("No bearer token provided")
            await websocket.close(1008, "Bearer token missing")
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        if websocket in active_connections:
            del active_connections[websocket]

async def main():
    print("Starting WebSocket server...")
    
    try:
        print("Setting up SSL context...")
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        try:
            ssl_context.load_cert_chain(
                '/home/bitnami/ssl/fullchain.pem',
                '/home/bitnami/ssl/privkey.pem'
            )
            print("SSL certificates loaded successfully")
        except Exception as e:
            print(f"Failed to load certificates: {e}")
            return

        server = await websockets.serve(
            handle_client,
            "0.0.0.0",
            8765,
            ssl=ssl_context,
            ping_interval=None,
            ping_timeout=None
        )
        
        print("Server running on port 8765 with SSL enabled")
        await asyncio.Future()
        
    except Exception as e:
        print(f"Server startup error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer shutdown")