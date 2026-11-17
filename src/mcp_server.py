import asyncio
import json
import logging
from typing import Annotated

from mcp import types
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError


class CustomerInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    customer_id: str = Field(pattern=r"^CUST-[A-Z0-9]{5}$", min_length=10, max_length=10)


class RefundInput(CustomerInput):
    amount: float = Field(gt=0, allow_inf_nan=False)
    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=10)]


server = Server("customer-tools")
tools = {
    "get_customer_record": (CustomerInput, "Return a demo customer record."),
    "trigger_refund": (RefundInput, "Accept a demo refund request."),
}


@server.list_tools()
async def list_tools():
    return [
        types.Tool(name=name, description=description, inputSchema=schema.model_json_schema())
        for name, (schema, description) in tools.items()
    ]


async def call_tool(request: types.CallToolRequest):
    tool = tools.get(request.params.name)
    if tool is None:
        raise McpError(types.ErrorData(code=types.INVALID_PARAMS, message="Unknown tool"))
    try:
        arguments = tool[0].model_validate(request.params.arguments or {})
    except ValidationError:
        raise McpError(types.ErrorData(code=types.INVALID_PARAMS, message="Invalid tool arguments")) from None

    result = arguments.model_dump()
    if isinstance(arguments, RefundInput):
        result["status"] = "accepted"
    else:
        result.update(name="Sample Customer", status="active")
    return types.ServerResult(types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(result))],
        structuredContent=result,
    ))


# The tool decorator converts exceptions into tool results instead of JSON-RPC errors.
server.request_handlers[types.CallToolRequest] = call_tool


async def main():
    async with stdio_server() as (reader, writer):
        await server.run(reader, writer, server.create_initialization_options())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
