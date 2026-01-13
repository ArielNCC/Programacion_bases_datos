/* ==========================================================
   PRY2206 - Semana 1
   Nicolás Cavieres
   ========================================================== */

-- Activar salida por consola
SET SERVEROUTPUT ON;
SET LINESIZE 200;
SET PAGESIZE 100;

/* ==========================================================
   CASO 1 – PROGRAMA DE PESOS TODOSUMA
   ========================================================== */

/* ----------------------------------------------------------
   Bloque PL/SQL para procesar un cliente
   ---------------------------------------------------------- */

DECLARE
    -- Entrada de usuario
    v_run_cliente NUMBER := &run_cliente;
    
    -- Valores de tramos y valores como constantes del programa
    v_tramo_1 NUMBER := 1000000;
    v_tramo_2 NUMBER := 3000000;
    v_peso_base NUMBER := 1200;
    v_extra_1 NUMBER := 100;
    v_extra_2 NUMBER := 300;
    v_extra_3 NUMBER := 550;
    
    -- Variables para datos del cliente
    v_nro_cliente CLIENTE.nro_cliente%TYPE;
    v_run_completo VARCHAR2(15);
    v_nombre_cliente VARCHAR2(100);
    v_tipo_cliente VARCHAR2(30);

    -- Variables para cálculos
    v_monto_total NUMBER := 0;
    v_tramos NUMBER := 0;
    v_pesos NUMBER := 0;
    v_pesos_extra NUMBER := 0;

    -- Variable para año dinámico
    v_anio_anterior NUMBER;
BEGIN
    -- Obtener año anterior dinámicamente
    v_anio_anterior := EXTRACT(YEAR FROM SYSDATE) - 1;

    -- Recuperar datos del cliente
    SELECT c.nro_cliente,
           c.numrun || '-' || c.dvrun,
           c.pnombre || ' ' || c.appaterno || ' ' || c.apmaterno,
           tc.nombre_tipo_cliente
    INTO v_nro_cliente, v_run_completo, v_nombre_cliente, v_tipo_cliente
    FROM CLIENTE c
    JOIN TIPO_CLIENTE tc ON c.cod_tipo_cliente = tc.cod_tipo_cliente
    WHERE c.numrun = v_run_cliente;

    -- Recuperar monto total de créditos del año anterior
    SELECT NVL(SUM(monto_solicitado), 0)
    INTO v_monto_total
    FROM CREDITO_CLIENTE
    WHERE nro_cliente = v_nro_cliente
      AND EXTRACT(YEAR FROM fecha_otorga_cred) = v_anio_anterior;

    -- Calcular cantidad de tramos de 100.000
    v_tramos := TRUNC(v_monto_total / 100000);

    -- Calcular pesos base
    v_pesos := v_tramos * v_peso_base;

    -- Calcular pesos extras según tipo de cliente
    IF v_tipo_cliente = 'Trabajadores independientes' THEN
        IF v_monto_total < v_tramo_1 THEN
            v_pesos_extra := v_tramos * v_extra_1;
        ELSIF v_monto_total <= v_tramo_2 THEN
            v_pesos_extra := v_tramos * v_extra_2;
        ELSE
            v_pesos_extra := v_tramos * v_extra_3;
        END IF;
    END IF;

    -- Totalizar pesos
    v_pesos := v_pesos + v_pesos_extra;

    -- Insertar resultado
    INSERT INTO CLIENTE_TODOSUMA
    (NRO_CLIENTE, RUN_CLIENTE, NOMBRE_CLIENTE,
     TIPO_CLIENTE, MONTO_SOLIC_CREDITOS, MONTO_PESOS_TODOSUMA)
    VALUES
    (v_nro_cliente, v_run_completo, v_nombre_cliente,
     v_tipo_cliente, v_monto_total, v_pesos);

    COMMIT;
    
    -- Evidencia de ejecución
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Cliente procesado: ' || v_nombre_cliente);
    DBMS_OUTPUT.PUT_LINE('RUN: ' || v_run_completo);
    DBMS_OUTPUT.PUT_LINE('Tipo: ' || v_tipo_cliente);
    DBMS_OUTPUT.PUT_LINE('Monto total: ' || v_monto_total);
    DBMS_OUTPUT.PUT_LINE('Tramos de $100.000: ' || v_tramos);
    DBMS_OUTPUT.PUT_LINE('Pesos base: ' || (v_tramos * v_peso_base));
    DBMS_OUTPUT.PUT_LINE('Pesos extra: ' || v_pesos_extra);
    DBMS_OUTPUT.PUT_LINE('TOTAL PESOS TODOSUMA: ' || v_pesos);
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('');

EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
        DBMS_OUTPUT.PUT_LINE('ADVERTENCIA: Cliente ya existe en CLIENTE_TODOSUMA.');
        DBMS_OUTPUT.PUT_LINE('Elimine el registro antes de volver a ejecutar.');
        DBMS_OUTPUT.PUT_LINE('');
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: No se encontró el cliente con RUN ' || v_run_cliente);
        DBMS_OUTPUT.PUT_LINE('');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error inesperado: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('');
        ROLLBACK;
END;
/

/* ==========================================================
   INSTRUCCIONES PARA CASO 1:
   
   Ejecutar el bloque anterior 5 veces con los siguientes RUNs:
   
   Cliente 1 - KAREN SOFIA PRADENAS MANDIOLA:
   run_cliente: 21242003
   
   Cliente 2 - SILVANA MARTINA VALENZUELA DUARTE:
   run_cliente: 22176845
   
   Cliente 3 - DENISSE ALICIA DIAZ MIRANDA:
   run_cliente: 18858542
   
   Cliente 4 - AMANDA ROMINA LIZANA MARAMBIO:
   run_cliente: 22558061
   
   Cliente 5 - LUIS CLAUDIO LUNA JORQUERA:
   run_cliente: 21300628
   ========================================================== */


/* ==========================================================
   CASO 2 – POSTERGACIÓN DE CUOTAS
   Bloque PL/SQL Anónimo con entrada de usuario
   ========================================================== */

/* ----------------------------------------------------------
   Bloque PL/SQL para procesar una solicitud de postergación
   (Ejecutar 3 veces, una por cada solicitud)
   ---------------------------------------------------------- */

DECLARE
    -- Entrada de usuario
    v_nro_cliente NUMBER := &numero_cliente;
    v_nro_solicitud NUMBER := &numero_solicitud;
    v_cant_cuotas NUMBER := &cantidad_cuotas;
    
    -- Variables para datos del crédito
    v_ultima_cuota NUMBER;
    v_fecha_ultima DATE;
    v_valor_cuota NUMBER;
    v_tasa NUMBER := 0;
    v_total_creditos NUMBER := 0;
    v_cod_credito NUMBER;
    
    -- Variable para iteración
    v_i NUMBER := 1;
BEGIN
    -- Obtener última cuota del crédito
    SELECT MAX(nro_cuota),
           MAX(fecha_venc_cuota),
           MAX(valor_cuota)
    INTO v_ultima_cuota, v_fecha_ultima, v_valor_cuota
    FROM CUOTA_CREDITO_CLIENTE
    WHERE nro_solic_credito = v_nro_solicitud;

    -- Obtener tipo de crédito
    SELECT cod_credito
    INTO v_cod_credito
    FROM CREDITO_CLIENTE
    WHERE nro_solic_credito = v_nro_solicitud;

    -- Definir tasa según tipo de crédito
    IF v_cod_credito = 1 THEN
        v_tasa := 0.005;  -- Hipotecario: 0.5%
    ELSIF v_cod_credito = 2 THEN
        v_tasa := 0.01;   -- Consumo: 1%
    ELSIF v_cod_credito = 3 THEN
        v_tasa := 0.02;   -- Automotriz: 2%
    ELSE
        v_tasa := 0;
    END IF;

    -- Contar créditos del año anterior
    SELECT COUNT(*)
    INTO v_total_creditos
    FROM CREDITO_CLIENTE
    WHERE nro_cliente = v_nro_cliente
      AND EXTRACT(YEAR FROM fecha_otorga_cred) = EXTRACT(YEAR FROM SYSDATE) - 1;

    -- Generar nuevas cuotas
    WHILE v_i <= v_cant_cuotas LOOP
        INSERT INTO CUOTA_CREDITO_CLIENTE
        VALUES (
            v_nro_solicitud,
            v_ultima_cuota + v_i,
            ADD_MONTHS(v_fecha_ultima, v_i),
            v_valor_cuota + (v_valor_cuota * v_tasa),
            NULL, NULL, NULL, NULL
        );
        v_i := v_i + 1;
    END LOOP;

    -- Condonar última cuota si corresponde
    IF v_total_creditos > 1 THEN
        UPDATE CUOTA_CREDITO_CLIENTE
        SET fecha_pago_cuota = fecha_venc_cuota,
            monto_pagado = valor_cuota,
            saldo_por_pagar = 0
        WHERE nro_solic_credito = v_nro_solicitud
          AND nro_cuota = v_ultima_cuota;
    END IF;

    COMMIT;
    
    -- Evidencia de ejecución
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Postergación procesada');
    DBMS_OUTPUT.PUT_LINE('Cliente: ' || v_nro_cliente);
    DBMS_OUTPUT.PUT_LINE('Crédito: ' || v_nro_solicitud);
    DBMS_OUTPUT.PUT_LINE('Última cuota original: ' || v_ultima_cuota);
    DBMS_OUTPUT.PUT_LINE('Cuotas postergadas: ' || v_cant_cuotas);
    DBMS_OUTPUT.PUT_LINE('Valor cuota original: ' || v_valor_cuota);
    DBMS_OUTPUT.PUT_LINE('Valor nueva cuota: ' || ROUND(v_valor_cuota + (v_valor_cuota * v_tasa)));
    DBMS_OUTPUT.PUT_LINE('Tasa aplicada: ' || (v_tasa * 100) || '%');
    DBMS_OUTPUT.PUT_LINE('Créditos año anterior: ' || v_total_creditos);
    IF v_total_creditos > 1 THEN
        DBMS_OUTPUT.PUT_LINE('Condonación: SÍ (última cuota original condonada)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Condonación: NO');
    END IF;
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('');

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: No se encontró el crédito ' || v_nro_solicitud);
        DBMS_OUTPUT.PUT_LINE('');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error inesperado: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('');
        ROLLBACK;
END;
/

/* ==========================================================
   INSTRUCCIONES PARA CASO 2:
   
   Ejecutar el bloque anterior 3 veces con los siguientes valores:
   
   Solicitud 1 - SEBASTIAN PATRICIO QUINTANA BERRIOS:
   numero_cliente: 1001
   numero_solicitud: 2001
   cantidad_cuotas: 2
   
   Solicitud 2 - KAREN SOFIA PRADENAS MANDIOLA:
   numero_cliente: 67
   numero_solicitud: 3004
   cantidad_cuotas: 1
   
   Solicitud 3 - JULIAN PAUL ARRIAGADA LUJAN:
   numero_cliente: 104
   numero_solicitud: 2004
   cantidad_cuotas: 1
   ========================================================== */


/* ==========================================================
   VERIFICACIÓN DE RESULTADOS
   ========================================================== */

/* ----------------------------------------------------------
   Verificación Caso 1: Tabla CLIENTE_TODOSUMA
   ---------------------------------------------------------- */

SELECT 
    NRO_CLIENTE,
    RUN_CLIENTE,
    NOMBRE_CLIENTE,
    TIPO_CLIENTE,
    MONTO_SOLIC_CREDITOS,
    MONTO_PESOS_TODOSUMA
FROM CLIENTE_TODOSUMA
ORDER BY NRO_CLIENTE;

/* ----------------------------------------------------------
   Verificación Caso 2: Cuotas Postergadas
   ---------------------------------------------------------- */

-- Crédito 2001 (últimas 3 cuotas)
SELECT 
    nro_solic_credito AS CREDITO,
    nro_cuota AS CUOTA,
    TO_CHAR(fecha_venc_cuota, 'DD/MM/YYYY') AS FECHA_VENC,
    valor_cuota AS VALOR,
    TO_CHAR(fecha_pago_cuota, 'DD/MM/YYYY') AS FECHA_PAGO,
    monto_pagado AS PAGADO
FROM CUOTA_CREDITO_CLIENTE
WHERE nro_solic_credito = 2001
  AND nro_cuota >= (SELECT MAX(nro_cuota) - 2 FROM CUOTA_CREDITO_CLIENTE WHERE nro_solic_credito = 2001)
ORDER BY nro_cuota;

-- Crédito 3004 (últimas 3 cuotas)
SELECT 
    nro_solic_credito AS CREDITO,
    nro_cuota AS CUOTA,
    TO_CHAR(fecha_venc_cuota, 'DD/MM/YYYY') AS FECHA_VENC,
    valor_cuota AS VALOR,
    TO_CHAR(fecha_pago_cuota, 'DD/MM/YYYY') AS FECHA_PAGO,
    monto_pagado AS PAGADO
FROM CUOTA_CREDITO_CLIENTE
WHERE nro_solic_credito = 3004
  AND nro_cuota >= (SELECT MAX(nro_cuota) - 2 FROM CUOTA_CREDITO_CLIENTE WHERE nro_solic_credito = 3004)
ORDER BY nro_cuota;

-- Crédito 2004 (últimas 3 cuotas)
SELECT 
    nro_solic_credito AS CREDITO,
    nro_cuota AS CUOTA,
    TO_CHAR(fecha_venc_cuota, 'DD/MM/YYYY') AS FECHA_VENC,
    valor_cuota AS VALOR,
    TO_CHAR(fecha_pago_cuota, 'DD/MM/YYYY') AS FECHA_PAGO,
    monto_pagado AS PAGADO
FROM CUOTA_CREDITO_CLIENTE
WHERE nro_solic_credito = 2004
  AND nro_cuota >= (SELECT MAX(nro_cuota) - 2 FROM CUOTA_CREDITO_CLIENTE WHERE nro_solic_credito = 2004)
ORDER BY nro_cuota;