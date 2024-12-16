# function_handlers.py
import aiohttp
import json

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

SUPABASE_ENDPOINT = "https://vxroqeomlzwpuwgrrspk.supabase.co/functions/v1/execute_sql"

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
