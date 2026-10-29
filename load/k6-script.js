import http from "k6/http";
import { check, sleep } from "k6";
import { Rate } from "k6/metrics";

const baseUrl = __ENV.BASE_URL || "http://localhost:8080";
const burnMilliseconds = __ENV.BURN_MS || "200";

const cpfs = [
  "11111111111",
  "22222222222",
  "33333333333",
  "44444444444",
  "55555555555",
];

const errorRate = new Rate("erros_de_negocio");

export const options = {
  scenarios: {
    rampup: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: 20 },
        { duration: "1m", target: 50 },
        { duration: "1m", target: 100 },
        { duration: "1m", target: 150 },
        { duration: "30s", target: 0 },
      ],
      gracefulStop: "15s",
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<800"],
    "http_req_duration{endpoint:score}": ["p(95)<500"],
    "http_req_duration{endpoint:consulta}": ["p(95)<500"],
    erros_de_negocio: ["rate<0.05"],
  },
};

function pickCpf() {
  return cpfs[Math.floor(Math.random() * cpfs.length)];
}

export default function () {
  const cpf = pickCpf();
  const draw = Math.random();

  if (draw < 0.5) {
    const response = http.get(`${baseUrl}/score/${cpf}`, {
      tags: { endpoint: "score" },
    });
    errorRate.add(response.status !== 200);
    check(response, { "score 200": (r) => r.status === 200 });
  } else if (draw < 0.85) {
    const response = http.get(`${baseUrl}/consulta/${cpf}`, {
      tags: { endpoint: "consulta" },
    });
    errorRate.add(response.status !== 200 && response.status !== 404);
    check(response, { "consulta ok": (r) => r.status === 200 || r.status === 404 });
  } else {
    const response = http.get(`${baseUrl}/burn?ms=${burnMilliseconds}`, {
      tags: { endpoint: "burn" },
    });
    errorRate.add(response.status !== 200);
    check(response, { "burn 200": (r) => r.status === 200 });
  }

  sleep(Math.random() * 0.5);
}
