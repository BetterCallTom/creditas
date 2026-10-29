INSERT INTO consulta_cadastral (cpf, nome, data_nascimento, cidade, uf, situacao) VALUES
    ('11111111111', 'Ana Beatriz Souza',   '1990-04-12', 'Sao Paulo',      'SP', 'REGULAR'),
    ('22222222222', 'Carlos Eduardo Lima',  '1985-11-23', 'Rio de Janeiro', 'RJ', 'REGULAR'),
    ('33333333333', 'Fernanda Oliveira',    '1998-07-05', 'Belo Horizonte', 'MG', 'PENDENTE'),
    ('44444444444', 'Joao Pedro Martins',   '1979-01-30', 'Curitiba',       'PR', 'REGULAR'),
    ('55555555555', 'Mariana Costa Ribeiro','2001-09-17', 'Recife',         'PE', 'NEGATIVADO')
ON CONFLICT (cpf) DO NOTHING;

INSERT INTO score (cpf, valor, faixa) VALUES
    ('11111111111', 920, 'excelente'),
    ('22222222222', 715, 'bom'),
    ('33333333333', 540, 'regular'),
    ('44444444444', 830, 'excelente'),
    ('55555555555', 380, 'baixo')
ON CONFLICT (cpf) DO NOTHING;
