CREATE TABLE IF NOT EXISTS consulta_cadastral (
    cpf             TEXT PRIMARY KEY,
    nome            TEXT NOT NULL,
    data_nascimento DATE NOT NULL,
    cidade          TEXT NOT NULL,
    uf              CHAR(2) NOT NULL,
    situacao        TEXT NOT NULL DEFAULT 'REGULAR',
    criado_em       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS score (
    cpf           TEXT PRIMARY KEY,
    valor         INTEGER NOT NULL CHECK (valor BETWEEN 300 AND 1000),
    faixa         TEXT NOT NULL,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
