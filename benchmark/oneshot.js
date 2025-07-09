const BASE_URL = "http://localhost:2084";
// const SIZE = 1024 * 102400; // 100 MB
const SIZE = 1;

async function main() {
  const key = Math.random().toString(36).substring(2, 12);
  const value = "x".repeat(SIZE);

  const encodedKey = btoa(key);
  const encodedValue = btoa(value);

  const response = await fetch(`${BASE_URL}/put`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key: encodedKey, value: encodedValue }),
  });

  const result = await response.text();
  console.log("Response:", result);
  console.log("Succeeded:", response.status === 200);

  const get_response = await fetch(`${BASE_URL}/get`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key: encodedKey }),
  });

  const get_result = await get_response.text();
  console.log("Get Response:", get_result);
  console.log("Get Succeeded:", get_response.status === 200);
}

main();
