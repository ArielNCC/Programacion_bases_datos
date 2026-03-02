/*====================================================================
  PROYECTO: PRY2206 – EXP 3 – SEMANA 8
  SISTEMA : GESTIÓN HOTELERA - Hotel "La Última Oportunidad"
  
  OBJETIVO:
     • CASO 1: Trigger de integridad para sincronizar TOTAL_CONSUMOS
     • CASO 2: Proceso integral de cobranza con package unificado
     
  =====================================================================
  REGLAS DE NEGOCIO IMPLEMENTADAS:
  =====================================================================
  RN-01: El monto de consumo no puede ser negativo
  RN-02: Solo se permiten consumos de huéspedes existentes
  RN-03: Descuento por tramo según tabla TRAMOS_CONSUMOS
  RN-04: Descuento de agencia según pct_agencia de tabla AGENCIA
  RN-05: Tours se muestran pero NO se suman al subtotal
  RN-06: Cálculos en USD, conversión final a CLP con redondeo
  RN-07: SUBTOTAL = ALOJAMIENTO + CONSUMOS + VALOR_PERSONA
  RN-08: TOTAL = SUBTOTAL - DESC_CONSUMO - DESC_AGENCIA
====================================================================*/

SET SERVEROUTPUT ON;

/*=====================================================================
  LIMPIEZA CONTROLADA (permite ejecutar el script múltiples veces)
=====================================================================*/
TRUNCATE TABLE reg_errores;

BEGIN EXECUTE IMMEDIATE 'DROP TRIGGER TRG_SINCRONIZAR_CONSUMOS'; 
EXCEPTION WHEN OTHERS THEN NULL; END;
/

BEGIN EXECUTE IMMEDIATE 'DROP PACKAGE PKG_HOTEL'; 
EXCEPTION WHEN OTHERS THEN NULL; END;
/

/*=====================================================================
  CASO 1: TRIGGER DE INTEGRIDAD (RN-01, RN-02)
  ---------------------------------------------------------------------
  Sincroniza automáticamente TOTAL_CONSUMOS cuando se modifica CONSUMO
  Eventos: INSERT, UPDATE, DELETE
  Validaciones: Monto no negativo, huésped existente
=====================================================================*/

CREATE OR REPLACE TRIGGER trg_sincronizar_consumos
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
DECLARE
    v_existe NUMBER;
    v_error  VARCHAR2(300);
BEGIN
    -- =========================================================
    -- Validación RN-01: Monto no puede ser NULL ni negativo
    -- =========================================================
    IF INSERTING OR UPDATING THEN
        IF :NEW.monto IS NULL OR :NEW.monto < 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Monto inválido (NULL o negativo)');
        END IF;
    END IF;

    -- =========================================================
    -- INSERT: Valida huésped y usa MERGE para upsert
    -- =========================================================
    IF INSERTING THEN
        SELECT COUNT(*) INTO v_existe FROM HUESPED WHERE id_huesped = :NEW.id_huesped;
        IF v_existe = 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'Huésped no existe: ' || :NEW.id_huesped);
        END IF;

        MERGE INTO total_consumos tc
        USING DUAL ON (tc.id_huesped = :NEW.id_huesped)
        WHEN MATCHED THEN
            UPDATE SET tc.monto_consumos = tc.monto_consumos + :NEW.monto
        WHEN NOT MATCHED THEN
            INSERT (id_huesped, monto_consumos) VALUES (:NEW.id_huesped, :NEW.monto);
    END IF;

    -- =========================================================
    -- UPDATE: Valida huésped y maneja cambio/diferencia de montos
    -- =========================================================
    IF UPDATING THEN
        -- Validar siempre que el huésped destino exista (RN-02)
        SELECT COUNT(*) INTO v_existe FROM HUESPED WHERE id_huesped = :NEW.id_huesped;
        IF v_existe = 0 THEN
            RAISE_APPLICATION_ERROR(-20003, 'Huésped no existe: ' || :NEW.id_huesped);
        END IF;
        
        IF :OLD.id_huesped <> :NEW.id_huesped THEN
            -- Restar del huésped anterior
            UPDATE total_consumos
            SET monto_consumos = GREATEST(monto_consumos - :OLD.monto, 0)
            WHERE id_huesped = :OLD.id_huesped;

            -- Sumar al nuevo huésped
            MERGE INTO total_consumos tc
            USING DUAL ON (tc.id_huesped = :NEW.id_huesped)
            WHEN MATCHED THEN
                UPDATE SET tc.monto_consumos = tc.monto_consumos + :NEW.monto
            WHEN NOT MATCHED THEN
                INSERT (id_huesped, monto_consumos) VALUES (:NEW.id_huesped, :NEW.monto);
        ELSE
            -- Mismo huésped, solo actualizar diferencia
            UPDATE total_consumos
            SET monto_consumos = GREATEST(monto_consumos - :OLD.monto + :NEW.monto, 0)
            WHERE id_huesped = :NEW.id_huesped;
        END IF;
    END IF;

    -- =========================================================
    -- DELETE: Resta monto evitando valores negativos
    -- =========================================================
    IF DELETING THEN
        UPDATE total_consumos
        SET monto_consumos = GREATEST(monto_consumos - :OLD.monto, 0)
        WHERE id_huesped = :OLD.id_huesped;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        v_error := SQLERRM;
        INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
        VALUES (sq_error.NEXTVAL, 'TRIGGER-TRG_SINCRONIZAR_CONSUMOS', v_error);
        RAISE;
END;
/

-- Verificar estado inicial
SELECT 'ESTADO INICIAL - CONSUMO' AS titulo FROM DUAL;
SELECT id_consumo, id_reserva, id_huesped, monto FROM consumo
WHERE id_huesped IN (340003, 340004, 340006, 340008, 340009) ORDER BY id_huesped, id_consumo;

SELECT 'ESTADO INICIAL - TOTAL_CONSUMOS' AS titulo FROM DUAL;
SELECT id_huesped, monto_consumos FROM total_consumos
WHERE id_huesped IN (340003, 340004, 340006, 340008, 340009) ORDER BY id_huesped;

/*=====================================================================
  BLOQUE DE PRUEBAS DEL TRIGGER
=====================================================================*/
DECLARE
    v_nuevo_id_consumo NUMBER;
    v_error VARCHAR2(300);
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== INICIO PRUEBAS TRIGGER ===');
    
    -- PRUEBA 1: INSERT (huésped 340006: 278 -> 428)
    BEGIN
        SELECT MAX(id_consumo) + 1 INTO v_nuevo_id_consumo FROM consumo;
        INSERT INTO consumo (id_consumo, id_reserva, id_huesped, monto)
        VALUES (v_nuevo_id_consumo, 1587, 340006, 150);
        DBMS_OUTPUT.PUT_LINE('PRUEBA 1 - INSERT: OK');
    EXCEPTION
        WHEN OTHERS THEN
            v_error := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('PRUEBA 1 - INSERT: ERROR - ' || v_error);
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-PRUEBA1_INSERT', v_error);
    END;
    
    -- PRUEBA 2: DELETE (huésped 340004: 158 -> 95)
    BEGIN
        DELETE FROM consumo WHERE id_consumo = 11473;
        IF SQL%ROWCOUNT = 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Consumo 11473 no encontrado');
        END IF;
        DBMS_OUTPUT.PUT_LINE('PRUEBA 2 - DELETE: OK');
    EXCEPTION
        WHEN OTHERS THEN
            v_error := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('PRUEBA 2 - DELETE: ERROR - ' || v_error);
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-PRUEBA2_DELETE', v_error);
    END;
    
    -- PRUEBA 3: UPDATE (huésped 340008: 211 -> 189)
    BEGIN
        UPDATE consumo SET monto = 95 WHERE id_consumo = 10688;
        IF SQL%ROWCOUNT = 0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'Consumo 10688 no encontrado');
        END IF;
        DBMS_OUTPUT.PUT_LINE('PRUEBA 3 - UPDATE: OK');
    EXCEPTION
        WHEN OTHERS THEN
            v_error := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('PRUEBA 3 - UPDATE: ERROR - ' || v_error);
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-PRUEBA3_UPDATE', v_error);
    END;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('=== PRUEBAS COMPLETADAS ===');
    
EXCEPTION
    WHEN OTHERS THEN
        v_error := SQLERRM;
        DBMS_OUTPUT.PUT_LINE('ERROR GENERAL PRUEBAS: ' || v_error);
        INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
        VALUES (sq_error.NEXTVAL, 'PROCEDURE-BLOQUE_PRUEBAS', v_error);
        ROLLBACK;
END;
/

-- Verificar estado final del trigger
SELECT 'ESTADO FINAL - CONSUMO' AS titulo FROM DUAL;
SELECT id_consumo, id_reserva, id_huesped, monto FROM consumo
WHERE id_huesped IN (340003, 340004, 340006, 340008, 340009) ORDER BY id_huesped, id_consumo;

SELECT 'ESTADO FINAL - TOTAL_CONSUMOS' AS titulo FROM DUAL;
SELECT id_huesped, monto_consumos FROM total_consumos
WHERE id_huesped IN (340003, 340004, 340006, 340008, 340009) ORDER BY id_huesped;

SELECT 'ERRORES CASO 1' AS titulo FROM DUAL;
SELECT id_error, nomsubprograma, msg_error FROM reg_errores ORDER BY id_error;

/*=====================================================================
  CASO 2: PROCESO INTEGRAL DE COBRANZA
  =====================================================================
  COMPONENTES DEL PACKAGE PKG_HOTEL:
  - fn_tours: Calcula total de tours por huésped
  - fn_consumos: Obtiene consumos de tabla TOTAL_CONSUMOS
  - fn_nom_agencia: Retorna nombre de agencia del huésped
  - fn_pct_agencia: Retorna porcentaje descuento de agencia (RN-04)
  - sp_proceso_cobranza: Procedimiento principal de cobranza
=====================================================================*/

CREATE OR REPLACE PACKAGE pkg_hotel IS
    -- =========================================================
    -- Funciones públicas del package
    -- =========================================================
    FUNCTION fn_tours(p_id_huesped NUMBER) RETURN NUMBER;
    FUNCTION fn_consumos(p_id_huesped NUMBER) RETURN NUMBER;
    FUNCTION fn_nom_agencia(p_id_huesped NUMBER) RETURN VARCHAR2;
    FUNCTION fn_pct_agencia(p_id_huesped NUMBER) RETURN NUMBER;
    PROCEDURE sp_proceso_cobranza(p_fecha_actual DATE, p_tipo_cambio NUMBER);
END pkg_hotel;
/

CREATE OR REPLACE PACKAGE BODY pkg_hotel IS

    -- =========================================================
    -- Constantes del package
    -- =========================================================
    c_valor_persona_clp CONSTANT NUMBER := 35000;

    /*-----------------------------------------------------------------
      FN_TOURS: Calcula el total de tours contratados por el huésped
      Regla: Tours se muestran pero NO se suman al subtotal (RN-05)
    -----------------------------------------------------------------*/
    FUNCTION fn_tours(p_id_huesped NUMBER) RETURN NUMBER IS
        v_total NUMBER;
        v_error VARCHAR2(300);
    BEGIN
        SELECT NVL(SUM(t.valor_tour * ht.num_personas), 0)
        INTO v_total
        FROM huesped_tour ht
        INNER JOIN tour t ON ht.id_tour = t.id_tour
        WHERE ht.id_huesped = p_id_huesped;
        
        RETURN v_total;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
        WHEN OTHERS THEN
            v_error := SQLERRM;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-FN_TOURS-ID:' || p_id_huesped, v_error);
            RETURN 0;
    END fn_tours;

    /*-----------------------------------------------------------------
      FN_CONSUMOS: Obtiene monto de consumos desde TOTAL_CONSUMOS
    -----------------------------------------------------------------*/
    FUNCTION fn_consumos(p_id_huesped NUMBER) RETURN NUMBER IS
        v_consumos NUMBER;
        v_error VARCHAR2(300);
    BEGIN
        SELECT monto_consumos INTO v_consumos
        FROM total_consumos WHERE id_huesped = p_id_huesped;
        RETURN v_consumos;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_error := 'Sin consumos registrados para huesped ' || p_id_huesped;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-FN_CONSUMOS-ID:' || p_id_huesped, v_error);
            RETURN 0;
        WHEN OTHERS THEN
            v_error := SQLERRM;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-FN_CONSUMOS-ID:' || p_id_huesped, v_error);
            RETURN 0;
    END fn_consumos;

    /*-----------------------------------------------------------------
      FN_NOM_AGENCIA: Retorna nombre de agencia del huésped
    -----------------------------------------------------------------*/
    FUNCTION fn_nom_agencia(p_id_huesped NUMBER) RETURN VARCHAR2 IS
        v_agencia VARCHAR2(40);
        v_error VARCHAR2(300);
    BEGIN
        SELECT a.nom_agencia INTO v_agencia
        FROM huesped h
        INNER JOIN agencia a ON h.id_agencia = a.id_agencia
        WHERE h.id_huesped = p_id_huesped;
        RETURN v_agencia;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_error := 'Huesped sin agencia asociada: ' || p_id_huesped;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-FN_NOM_AGENCIA-ID:' || p_id_huesped, v_error);
            RETURN 'SIN AGENCIA';
        WHEN OTHERS THEN
            v_error := SQLERRM;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-FN_NOM_AGENCIA-ID:' || p_id_huesped, v_error);
            RETURN 'SIN AGENCIA';
    END fn_nom_agencia;

    /*-----------------------------------------------------------------
      FN_PCT_AGENCIA: Retorna porcentaje de descuento de agencia (RN-04)
      Lee directamente de tabla AGENCIA.pct_agencia
    -----------------------------------------------------------------*/
    FUNCTION fn_pct_agencia(p_id_huesped NUMBER) RETURN NUMBER IS
        v_pct NUMBER;
        v_error VARCHAR2(300);
    BEGIN
        SELECT NVL(a.pct_agencia, 0) INTO v_pct
        FROM huesped h
        INNER JOIN agencia a ON h.id_agencia = a.id_agencia
        WHERE h.id_huesped = p_id_huesped;
        RETURN v_pct;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_error := 'Sin porcentaje agencia para huesped: ' || p_id_huesped;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-FN_PCT_AGENCIA-ID:' || p_id_huesped, v_error);
            RETURN 0;
        WHEN OTHERS THEN
            v_error := SQLERRM;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'FUNCION-FN_PCT_AGENCIA-ID:' || p_id_huesped, v_error);
            RETURN 0;
    END fn_pct_agencia;

    /*-----------------------------------------------------------------
      SP_PROCESO_COBRANZA: Procedimiento principal de cobranza
      -----------------------------------------------------------------
      Parámetros:
        p_fecha_actual: Fecha de checkout a procesar
        p_tipo_cambio: Tipo de cambio USD a CLP
      
      Proceso:
        1. Valida parámetros
        2. Verifica idempotencia por fecha (RN-09)
        3. Para cada huésped con checkout en la fecha:
           - Calcula alojamiento, consumos, tours
           - Aplica descuento por tramo (RN-03)
           - Aplica descuento agencia parametrizado (RN-04)
           - Convierte a CLP y redondea (RN-06)
        4. Inserta en DETALLE_DIARIO_HUESPEDES
    -----------------------------------------------------------------*/
    PROCEDURE sp_proceso_cobranza(p_fecha_actual DATE, p_tipo_cambio NUMBER) IS
        CURSOR c_huespedes IS
            SELECT 
                r.id_huesped,
                h.appat_huesped || ' ' || h.nom_huesped AS nombre,
                r.estadia,
                hab.tipo_habitacion,
                hab.valor_habitacion,
                hab.valor_minibar
            FROM reserva r
            INNER JOIN huesped h ON r.id_huesped = h.id_huesped
            INNER JOIN detalle_reserva dr ON r.id_reserva = dr.id_reserva
            INNER JOIN habitacion hab ON dr.id_habitacion = hab.id_habitacion
            WHERE r.ingreso + r.estadia = p_fecha_actual
            GROUP BY r.id_huesped, h.appat_huesped, h.nom_huesped,
                     r.estadia, hab.tipo_habitacion,
                     hab.valor_habitacion, hab.valor_minibar;
        
        v_num_personas          NUMBER;
        v_valor_persona_usd     NUMBER;
        v_alojamiento_usd       NUMBER;
        v_consumos_usd          NUMBER;
        v_tours_usd             NUMBER;
        v_subtotal_usd          NUMBER;
        v_pct_consumos          NUMBER;
        v_descuento_consumos    NUMBER;
        v_pct_agencia           NUMBER;
        v_descuento_agencia     NUMBER;
        v_total_usd             NUMBER;
        v_nom_agencia           VARCHAR2(40);
        v_error                 VARCHAR2(300);
        v_contador              NUMBER := 0;
        
    BEGIN
        -- =========================================================
        -- Validación de parámetros de entrada
        -- =========================================================
        IF p_fecha_actual IS NULL THEN
            RAISE_APPLICATION_ERROR(-20010, 'Fecha no puede ser nula');
        END IF;
        IF p_tipo_cambio IS NULL OR p_tipo_cambio <= 0 THEN
            RAISE_APPLICATION_ERROR(-20011, 'Tipo de cambio debe ser mayor a 0');
        END IF;

        -- =========================================================
        -- Limpiar datos previos antes de procesar
        -- (La tabla almacena solo el proceso del día actual)
        -- =========================================================
        DELETE FROM detalle_diario_huespedes;
        
        DBMS_OUTPUT.PUT_LINE('=== INICIO PROCESO COBRANZA ===');
        DBMS_OUTPUT.PUT_LINE('Fecha: ' || TO_CHAR(p_fecha_actual, 'DD/MM/YYYY') || 
                             ' | TC: $' || p_tipo_cambio);
        
        FOR reg IN c_huespedes LOOP
            BEGIN
                -- =========================================================
                -- Determinar número de personas según tipo habitación
                -- =========================================================
                CASE reg.tipo_habitacion
                    WHEN 'S' THEN v_num_personas := 1;  -- Single
                    WHEN 'D' THEN v_num_personas := 2;  -- Doble
                    WHEN 'T' THEN v_num_personas := 3;  -- Triple
                    WHEN 'C' THEN v_num_personas := 4;  -- Cuádruple
                    ELSE v_num_personas := 1;
                END CASE;
                
                -- =========================================================
                -- Cálculos en USD (usa constante c_valor_persona_clp)
                -- =========================================================
                v_valor_persona_usd := ROUND(c_valor_persona_clp / p_tipo_cambio, 2) * v_num_personas;
                v_alojamiento_usd := (reg.valor_habitacion + reg.valor_minibar) * reg.estadia;
                v_consumos_usd := fn_consumos(reg.id_huesped);
                v_tours_usd := fn_tours(reg.id_huesped);
                v_nom_agencia := fn_nom_agencia(reg.id_huesped);
                v_pct_agencia := fn_pct_agencia(reg.id_huesped);
                
                -- =========================================================
                -- SUBTOTAL = Alojamiento + Consumos + Valor Persona (RN-07)
                -- Nota: Tours NO se incluyen en subtotal (RN-05)
                -- =========================================================
                v_subtotal_usd := v_alojamiento_usd + v_consumos_usd + v_valor_persona_usd;
                
                -- =========================================================
                -- Descuento por tramo de consumos (RN-03)
                -- Usa MAX para evitar ORA-01422 si hay tramos superpuestos
                -- =========================================================
                SELECT NVL(MAX(pct), 0) INTO v_pct_consumos 
                FROM tramos_consumos
                WHERE v_consumos_usd BETWEEN vmin_tramo AND vmax_tramo;
                
                v_descuento_consumos := v_consumos_usd * v_pct_consumos;
                
                -- =========================================================
                -- Descuento agencia parametrizado (RN-04)
                -- Usa pct_agencia de tabla AGENCIA (no hardcodeado)
                -- =========================================================
                v_descuento_agencia := v_subtotal_usd * v_pct_agencia;
                
                -- =========================================================
                -- TOTAL = Subtotal - Desc.Consumos - Desc.Agencia (RN-08)
                -- =========================================================
                v_total_usd := v_subtotal_usd - v_descuento_consumos - v_descuento_agencia;
                
                -- =========================================================
                -- INSERT con conversión a CLP y redondeo (RN-06)
                -- Orden: fecha_proceso, id_huesped, nombre, agencia...
                -- =========================================================
                INSERT INTO detalle_diario_huespedes (
                    id_huesped,
                    nombre,
                    agencia,
                    alojamiento,
                    consumos,
                    tours,
                    subtotal_pago,
                    descuento_consumos,
                    descuentos_agencia,
                    total
                ) VALUES (
                    reg.id_huesped,
                    reg.nombre,
                    v_nom_agencia,
                    ROUND(v_alojamiento_usd * p_tipo_cambio),
                    ROUND(v_consumos_usd * p_tipo_cambio),
                    ROUND(v_tours_usd * p_tipo_cambio),
                    ROUND(v_subtotal_usd * p_tipo_cambio),
                    ROUND(v_descuento_consumos * p_tipo_cambio),
                    ROUND(v_descuento_agencia * p_tipo_cambio),
                    ROUND(v_total_usd * p_tipo_cambio)
                );
                
                v_contador := v_contador + 1;
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_error := SQLERRM;
                    INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
                    VALUES (sq_error.NEXTVAL, 'PROCEDURE-SP_COBRANZA-ID:' || reg.id_huesped, v_error);
            END;
        END LOOP;
        
        -- Verificar si se procesaron huéspedes
        IF v_contador = 0 THEN
            DBMS_OUTPUT.PUT_LINE('ADVERTENCIA: No hay huéspedes con checkout para la fecha ' || 
                                 TO_CHAR(p_fecha_actual, 'DD/MM/YYYY'));
        ELSE
            DBMS_OUTPUT.PUT_LINE('Huéspedes procesados: ' || v_contador);
        END IF;
        
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('=== PROCESO FINALIZADO ===');
        
    EXCEPTION
        WHEN OTHERS THEN
            v_error := SQLERRM;
            ROLLBACK;
            INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
            VALUES (sq_error.NEXTVAL, 'PROCEDURE-SP_COBRANZA-FATAL', v_error);
            COMMIT;
            RAISE;
    END sp_proceso_cobranza;
    
END pkg_hotel;
/

/*=====================================================================
  EJECUCIÓN DEL PROCESO
=====================================================================*/
ACCEPT v_tipo_cambio NUMBER PROMPT 'Ingrese el tipo de cambio en pesos chilenos (915): '
ACCEPT v_fecha CHAR PROMPT 'Ingrese la fecha actual DD/MM/AAAA (18/08/2021 para pruebas): '

BEGIN
    pkg_hotel.sp_proceso_cobranza(
        p_fecha_actual => TO_DATE('&&v_fecha', 'DD/MM/YYYY'),
        p_tipo_cambio  => &&v_tipo_cambio
    );
END;
/

/*=====================================================================
  VERIFICACIÓN DE RESULTADOS
=====================================================================*/
SELECT 'DETALLE_DIARIO_HUESPEDES' AS titulo FROM DUAL;
SELECT id_huesped, nombre, agencia, alojamiento, consumos, tours,
       subtotal_pago, descuento_consumos, descuentos_agencia, total
FROM detalle_diario_huespedes ORDER BY id_huesped;

SELECT 'REG_ERRORES' AS titulo FROM DUAL;
SELECT id_error, nomsubprograma, msg_error FROM reg_errores ORDER BY id_error;