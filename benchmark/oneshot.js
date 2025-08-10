const SIZE = 1;

function usage() {
  console.log("Usage: node oneshot.js <write|read> [key] [base_url]");
  process.exit(1);
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length < 1) usage();
  const mode = args[0];
  let baseUrl = "http://localhost:2084";
  if (args.length > 2) {
    baseUrl = args[2];
  }

  if (mode === "write") {
    const key = Math.random().toString(36).substring(2, 12);
    const value = "x".repeat(SIZE);
    const encodedKey = btoa(key);
    const encodedValue = btoa(value);

    const response = await fetch(`${baseUrl}/put`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key: encodedKey, value: encodedValue }),
    });
    const result = await response.text();
    console.log("Response:", result);
    console.log("Succeeded:", response.status === 200);
    console.log("Key:", key);
  } else if (mode === "read") {
    if (args.length < 2) usage();
    const key = args[1];
    const get_response = await fetch(`${baseUrl}/get`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key: key }),
    });
    const get_result = await get_response.text();
    console.log("Get Response:", get_result);
    console.log("Get Succeeded:", get_response.status === 200);
  } else {
    usage();
  }
}

main();
