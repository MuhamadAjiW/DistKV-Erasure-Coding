import http from 'k6/http';
import { check, sleep } from 'k6';
import { b64encode } from 'k6/encoding';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:2379/v3';
const VUS = __ENV.VUS || 100;
const DURATION = __ENV.DURATION || '10s';

export const options = {
    vus: VUS,
    duration: DURATION,
};

export default function () {
    const key = Math.random().toString(36).substring(2, 12);
    const value = Math.random().toString(36).substring(2, 12);

    const encodedKey = b64encode(key);
    const encodedValue = b64encode(value);

    const putRes = http.post(`${BASE_URL}/kv/put`, JSON.stringify({ key: encodedKey, value: encodedValue }), {
        headers: { 'Content-Type': 'application/json' },
    });
    check(putRes, { 'PUT succeeded': (r) => r.status === 200 });

    // console.log('PUT Response Status:', putRes.status);
    // console.log('PUT Response Body:', putRes.body);

    const getRes = http.post(`${BASE_URL}/kv/range`, JSON.stringify({ key: encodedKey }), {
        headers: { 'Content-Type': 'application/json' },
    });
    check(getRes, { 'GET succeeded': (r) => r.status === 200 });
    
    // console.log('GET Response Status:', getRes.status);
    // console.log('GET Response Body:', getRes.body);
    
    const delRes = http.post(`${BASE_URL}/kv/deleterange`, JSON.stringify({ key: encodedKey }), {
        headers: { 'Content-Type': 'application/json' },
    });
    check(delRes, { 'GET succeeded': (r) => r.status === 200 });

    // console.log('DELETE Response Status:', delRes.status);
    // console.log('DELETE Response Body:', delRes.body);

    sleep(0.1);
}
