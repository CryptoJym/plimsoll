import { authenticatedJsonPost, MAX_POST_BYTES, TransportError, type JsonPostOptions } from "./http-transport";
import { DELIVERY_ACK_HEADER, deliveryExpectation, validateDeliveryAcknowledgement } from "./delivery-ack";

/** Strict clients request v1; an older server may store the batch but cannot
 * clear local retry state. Roll out the additive server before this client.
 */
export async function postDelivery(input: JsonPostOptions & {
  installKey: string; ingestKey?: string; signingSecret?: string; now?: () => Date;
}) {
  if (Buffer.byteLength(input.body) > MAX_POST_BYTES) throw new TransportError("request_too_large");
  const expected = deliveryExpectation(input.body, input.installKey);
  const response = await authenticatedJsonPost({ ...input, headers: { ...input.headers, [DELIVERY_ACK_HEADER]: "1" } });
  const acknowledgement = response.ok
    ? validateDeliveryAcknowledgement(response.body, expected)
    : null;
  return { ...response, acknowledgement };
}
