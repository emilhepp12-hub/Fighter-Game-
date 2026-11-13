import { DurableObject } from "cloudflare:workers";

/* Storm Jet · Vermittler für Online-Spiele.
   Jeder Raumcode ist ein eigenes kleines Objekt (Room). Host und Mitspieler
   verbinden sich per WebSocket mit uns; wir reichen jede Nachricht an den anderen weiter. */

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const m = url.pathname.match(/^\/ws\/([A-Za-z0-9]{5})$/);
    if (m) {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("WebSocket erwartet", { status: 426 });
      const stub = env.ROOMS.get(env.ROOMS.idFromName(m[1].toUpperCase()));
      return stub.fetch(req);
    }
    return env.ASSETS.fetch(req);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(req) {
    const role = new URL(req.url).searchParams.get("role") === "host" ? "host" : "guest";
    const [client, server] = Object.values(new WebSocketPair());
    const hosts = this.ctx.getWebSockets("host");
    const guests = this.ctx.getWebSockets("guest");

    const reject = (k) => {
      this.ctx.acceptWebSocket(server, ["x"]);
      server.send(JSON.stringify({ _k: k }));
      server.close(1000, k);
    };

    if (role === "host") {
      if (hosts.length) reject("taken");
      else {
        this.ctx.acceptWebSocket(server, ["host"]);
        server.send(JSON.stringify({ _k: "ready" }));
      }
    } else if (!hosts.length) reject("nohost");
    else if (guests.length) reject("full");
    else {
      this.ctx.acceptWebSocket(server, ["guest"]);
      const hello = JSON.stringify({ _k: "peer" });
      server.send(hello);
      for (const h of hosts) h.send(hello);
    }
    return new Response(null, { status: 101, webSocket: client });
  }

  other(ws) {
    const tags = this.ctx.getTags(ws);
    if (tags.includes("host")) return this.ctx.getWebSockets("guest");
    if (tags.includes("guest")) return this.ctx.getWebSockets("host");
    return [];
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > 4096) return;
    for (const o of this.other(ws)) {
      try { o.send(msg); } catch (e) {}
    }
  }

  gone(ws) {
    const bye = JSON.stringify({ _k: "bye" });
    for (const o of this.other(ws)) {
      try { o.send(bye); } catch (e) {}
    }
    try { ws.close(1000, "bye"); } catch (e) {}
  }

  async webSocketClose(ws) { this.gone(ws); }
  async webSocketError(ws) { this.gone(ws); }
}
