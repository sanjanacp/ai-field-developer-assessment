import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

const customerId = z.string().regex(/^CUST-[A-Z0-9]{5}$/);
const server = new McpServer({ name: "customer-tools", version: "1.0.0" });

server.registerTool("get_customer_record", {
  description: "Return a customer record by ID.",
  inputSchema: z.object({ customer_id: customerId }).strict(),
}, async ({ customer_id }) => ({
  content: [{ type: "text", text: JSON.stringify({ customer_id, name: "Sample Customer", status: "active" }) }],
}));

server.registerTool("trigger_refund", {
  description: "Record a refund request for a customer.",
  inputSchema: z.object({
    customer_id: customerId,
    amount: z.number().finite().positive(),
    reason: z.string().trim().min(10),
  }).strict(),
}, async ({ customer_id, amount, reason }) => ({
  content: [{ type: "text", text: JSON.stringify({ customer_id, amount, reason, status: "accepted" }) }],
}));

try {
  await server.connect(new StdioServerTransport());
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
