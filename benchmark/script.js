import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;

export const options = {
    vus: 100,
    duration: '30s',
};

export default function () {
    const key = Math.random().toString(36).substring(2, 12);
    const value = JSON.stringify({ val: Math.random() });

    const putRes = http.post(`${BASE_URL}/kv/put`, JSON.stringify({ key, value }), {
        headers: { 'Content-Type': 'application/json' },
    });
    check(putRes, { 'PUT succeeded': (r) => r.status === 200 });

    const getRes = http.post(`${BASE_URL}/kv/range`, JSON.stringify({ key }), {
        headers: { 'Content-Type': 'application/json' },
    });
    check(getRes, { 'GET succeeded': (r) => r.status === 200 });
    
    const delRes = http.post(`${BASE_URL}/kv/deleterange`, JSON.stringify({ key }), {
        headers: { 'Content-Type': 'application/json' },
    });
    check(delRes, { 'GET succeeded': (r) => r.status === 200 });

    sleep(0.1);
}
