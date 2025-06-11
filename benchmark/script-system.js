import http from 'k6/http';
import { check, sleep } from 'k6';
import { b64encode } from 'k6/encoding';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:2084';
const VUS = __ENV.VUS || 1;
const SIZE = __ENV.SIZE || 10240;
const DURATION = __ENV.DURATION || '30s';

export const options = {
    vus: VUS,
    duration: DURATION,
};

export default function () {
    const key = Math.random().toString(36).substring(2, 12);
    const value = 'x'.repeat(SIZE);

    const encodedKey = b64encode(key);
    const encodedValue = b64encode(value);

    const putRes = http.post(`${BASE_URL}/put`, JSON.stringify({ key: encodedKey, value: encodedValue }), {
        headers: { 'Content-Type': 'application/json' },
    });
    check(putRes, { 'PUT succeeded': (r) => r.status === 200 });

    // console.log('PUT Response Status:', putRes.status);
    // console.log('PUT Response Body:', putRes.body);

    // const getRes = http.post(`${BASE_URL}/kv/range`, JSON.stringify({ key: encodedKey }), {
    //     headers: { 'Content-Type': 'application/json' },
    // });
    // check(getRes, { 'GET succeeded': (r) => r.status === 200 });
    
    // console.log('GET Response Status:', getRes.status);
    // console.log('GET Response Body:', getRes.body);
    
    // const delRes = http.post(`${BASE_URL}/kv/deleterange`, JSON.stringify({ key: encodedKey }), {
    //     headers: { 'Content-Type': 'application/json' },
    // });
    // check(delRes, { 'DELETE succeeded': (r) => r.status === 200 });

    // console.log('DELETE Response Status:', delRes.status);
    // console.log('DELETE Response Body:', delRes.body);

    // sleep(1);
}

export function handleSummary(data) {
    const summaryOutput = {
        testRunDetails: {
            timestamp: new Date().toISOString(),
            scriptOptions: options, 
            environmentVariables: {
                BASE_URL: BASE_URL,
                VUS: __ENV.VUS || VUS.toString(), 
                SIZE: __ENV.SIZE || SIZE.toString(),
                DURATION: __ENV.DURATION || DURATION,
            },
        },
        summary: {},
        details: {}
    };

    for (const metricName in data.metrics) {
        if (data.metrics.hasOwnProperty(metricName)) {
            const metric = data.metrics[metricName];
            summaryOutput.details[metricName] = {
                type: metric.type,
                contains: metric.contains,
                values: metric.values, // Contains avg, min, max, p90, p95, etc. for trends/rates
            };
        }
    }
    
    if (data.metrics['http_req_duration{expected_response:true}']) {
        summaryOutput.summary.success_performance = data.metrics['http_req_duration{expected_response:true}'].values;
    }
    
    if (data.metrics.http_reqs) {
        summaryOutput.summary.reqs = data.metrics.http_reqs.values;
    }

    return {
        'stdout': JSON.stringify(summaryOutput, null, 2),
    };
}
