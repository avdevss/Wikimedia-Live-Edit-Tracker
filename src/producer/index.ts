const STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

async function main() {
  const res = await fetch(STREAM_URL, {
    headers: {
      Accept: "text/event-stream",
      "User-Agent":
        "trending-leaderboard/0.1 (https://github.com/avdevss/trending-leaderboard)",
    },
  });

  if (!res.ok || !res.body) {
    throw new Error(`stream connect failed: ${res.status} ${res.statusText}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    let boundary: number;
    while ((boundary = buffer.indexOf("\n\n")) !== -1) {
      const rawEvent = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);

      for (const line of rawEvent.split("\n")) {
        if (line.startsWith("data: ")) {
          const payload = JSON.parse(line.slice("data: ".length));
          console.log(payload);
        }
      }
    }
  }
}

main();
