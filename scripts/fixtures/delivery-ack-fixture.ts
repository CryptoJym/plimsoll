import { deliveryAcknowledgement, deliveryExpectation } from "../../packages/collector-cli/src/delivery-ack";

/** Upgrade a positive mock server response to the additive v1 wire protocol.
 * Refusals and inconsistent legacy counts remain untouched. Fault injection
 * in delivery-transport-proof deliberately does not use this fixture adapter.
 */
export function acknowledgingFetch(impl: typeof fetch): typeof fetch {
  return async (input, init) => {
    const response = await impl(input, init);
    if (!response.ok) return response;
    const body = await response.clone().json().catch(() => null);
    if (!body || typeof body !== "object" || typeof body.accepted !== "number" || body.ack !== undefined) return response;
    const rawBody = String(init?.body ?? "");
    const payload = JSON.parse(rawBody);
    const expected = deliveryExpectation(rawBody, payload.installKey);
    if (body.accepted !== expected.itemIds.length) return response;
    const upgraded = new Response(JSON.stringify({ ...body, ack: deliveryAcknowledgement(expected, expected.itemIds) }), {
      status: response.status, headers: response.headers,
    });
    Object.defineProperties(upgraded, { url: { value: response.url }, redirected: { value: response.redirected } });
    return upgraded;
  };
}
