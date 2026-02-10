-- Habilita salida por consola para ver mensajes DBMS_OUTPUT.
SET SERVEROUTPUT ON

/* ============================================================
   BIND VARIABLE – AÑO DE EJECUCIÓN
   ============================================================ */
VARIABLE b_anno NUMBER
-- Solicita el año al usuario (SQL*Plus) y lo deja en la variable bind.
EXEC :b_anno := &anno

/* ============================================================
   BLOQUE PL/SQL – APORTE SBIF (VERSIÓN FINAL CORREGIDA)
   ============================================================ */
DECLARE
   v_anno_input      NUMBER;
   v_anno_proceso    NUMBER;

   v_fecha_ini       DATE;
   v_fecha_fin       DATE;

   v_total_registros NUMBER := 0;
   v_procesados      NUMBER := 0;

   v_total_resumen   NUMBER := 0;
   v_res_insertados  NUMBER := 0;

   /* ===============================
      VARRAY TIPOS
      =============================== */
   TYPE t_tipos IS VARRAY(2) OF VARCHAR2(40);
   v_tipos t_tipos := t_tipos(
      'Avance en Efectivo',
      'Súper Avance en Efectivo'
   );

   /* ===============================
      RECORD TRANSACCIÓN
      =============================== */
   TYPE r_trx IS RECORD (
      numrun            CLIENTE.numrun%TYPE,
      dvrun             CLIENTE.dvrun%TYPE,
      nro_tarjeta       TARJETA_CLIENTE.nro_tarjeta%TYPE,
      nro_transaccion   TRANSACCION_TARJETA_CLIENTE.nro_transaccion%TYPE,
      fecha_transaccion TRANSACCION_TARJETA_CLIENTE.fecha_transaccion%TYPE,
      tipo_transaccion  VARCHAR2(40),
      monto_base        TRANSACCION_TARJETA_CLIENTE.monto_transaccion%TYPE,
      tasa_interes      TIPO_TRANSACCION_TARJETA.tasaint_tptran_tarjeta%TYPE
   );

   v_trx r_trx;

   v_porcentaje_sbif NUMBER;
   v_aporte_sbif     NUMBER;
   v_es_valida       BOOLEAN;

   v_monto_total     NUMBER;

   /* ===============================
      EXCEPCIONES
      - Predefinida: NO_DATA_FOUND
      - No predefinida: ORA-01722 (invalid number)
      - Usuario: e_sin_transacciones, e_diferencia_conteo
      =============================== */
   e_sin_transacciones EXCEPTION;
   e_diferencia_conteo EXCEPTION;

   e_invalid_number EXCEPTION;
   PRAGMA EXCEPTION_INIT(e_invalid_number, -1722);

   /* ===============================
      CURSOR DETALLE (ROBUSTO FECHAS)
      =============================== */
   CURSOR c_detalle IS
      SELECT
         c.numrun,
         c.dvrun,
         t.nro_tarjeta,
         tr.nro_transaccion,
         tr.fecha_transaccion,
         tp.nombre_tptran_tarjeta,
          tr.monto_transaccion,
          tp.tasaint_tptran_tarjeta
      FROM cliente c
      JOIN tarjeta_cliente t
           ON c.numrun = t.numrun
      JOIN transaccion_tarjeta_cliente tr
           ON t.nro_tarjeta = tr.nro_tarjeta
      JOIN tipo_transaccion_tarjeta tp
           ON tr.cod_tptran_tarjeta = tp.cod_tptran_tarjeta
      WHERE tr.fecha_transaccion >= v_fecha_ini
        AND tr.fecha_transaccion <  v_fecha_fin
        AND tp.nombre_tptran_tarjeta IN (v_tipos(1), v_tipos(2))
      ORDER BY tr.fecha_transaccion, c.numrun;

   /* ===============================
      CURSOR RESUMEN (CON PARÁMETRO)
      Inserta en orden ascendente por año/mes y tipo.
      =============================== */
   CURSOR c_resumen(p_yyyymm VARCHAR2) IS
       SELECT tipo_transaccion,
            monto_transaccion,
            aporte_sbif
         FROM detalle_aporte_sbif
        WHERE TO_CHAR(fecha_transaccion, 'YYYYMM') = p_yyyymm
        ORDER BY tipo_transaccion, fecha_transaccion, numrun;

   v_res_tipo          RESUMEN_APORTE_SBIF.tipo_transaccion%TYPE;
   v_res_monto_total   RESUMEN_APORTE_SBIF.monto_total_transacciones%TYPE;
   v_res_aporte_total  RESUMEN_APORTE_SBIF.aporte_total_abif%TYPE;
   v_res_tipo_actual   RESUMEN_APORTE_SBIF.tipo_transaccion%TYPE;
   v_res_monto_acum    NUMBER;
   v_res_aporte_acum   NUMBER;
   v_res_monto_det     NUMBER;
   v_res_aporte_det    NUMBER;

   /* ===============================
      LOG ROBUSTO (TRANSACCIÓN AUTÓNOMA)
      =============================== */
   PROCEDURE log_error(
      p_descripcion IN VARCHAR2,
      p_cod_error   IN NUMBER,
      p_msg_error   IN VARCHAR2
   ) IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      INSERT INTO error_log(fecha_error, descripcion, cod_error, msg_error)
      VALUES (SYSDATE, SUBSTR(p_descripcion, 1, 500), p_cod_error, SUBSTR(p_msg_error, 1, 200));
      COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END;

BEGIN
   /* ===============================
      AÑO Y RANGO DE FECHAS
      =============================== */
   -- Año ingresado por el usuario.
   v_anno_input   := :b_anno;
   -- Se procesa el año anterior al ingresado (regla del enunciado).
   v_anno_proceso := v_anno_input - 1;

   -- Define rango [01-01, 01-01 del año siguiente) para filtrar por fecha.
   v_fecha_ini := DATE '2000-01-01' + NUMTOYMINTERVAL(v_anno_proceso - 2000, 'YEAR');
   v_fecha_fin := ADD_MONTHS(v_fecha_ini, 12);

   /* ===============================
      LIMPIEZA
      =============================== */
   -- Se reinician tablas de trabajo para asegurar una corrida limpia.
   EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_aporte_sbif';
   EXECUTE IMMEDIATE 'TRUNCATE TABLE resumen_aporte_sbif';
   BEGIN
      -- El log puede no existir o no estar permitido truncar; no detiene el proceso.
      EXECUTE IMMEDIATE 'TRUNCATE TABLE error_log';
   EXCEPTION
      WHEN OTHERS THEN
         NULL;
   END;

   -- Punto de retorno: permite rollback parcial ante errores controlados.
   SAVEPOINT sp_proceso;

   /* ===============================
      CONTEO PREVIO
      =============================== */
   -- Cuenta transacciones objetivo para validar consistencia vs. las procesadas.
   SELECT COUNT(*)
   INTO v_total_registros
   FROM transaccion_tarjeta_cliente tr
   JOIN tipo_transaccion_tarjeta tp
        ON tr.cod_tptran_tarjeta = tp.cod_tptran_tarjeta
   WHERE tr.fecha_transaccion >= v_fecha_ini
     AND tr.fecha_transaccion <  v_fecha_fin
     AND tp.nombre_tptran_tarjeta IN (
         'Avance en Efectivo',
         'Súper Avance en Efectivo'
     );

   /* ===============================
      PROCESO DETALLE
      =============================== */
   -- Recorre transacciones del periodo y tipos definidos (Avance / Súper Avance).
   OPEN c_detalle;
   LOOP
      FETCH c_detalle INTO
         v_trx.numrun,
         v_trx.dvrun,
         v_trx.nro_tarjeta,
         v_trx.nro_transaccion,
         v_trx.fecha_transaccion,
         v_trx.tipo_transaccion,
         v_trx.monto_base,
         v_trx.tasa_interes;

      EXIT WHEN c_detalle%NOTFOUND;

      -- Monto total: base + interés (si tasa viene NULL se considera 0).
      v_monto_total := ROUND(v_trx.monto_base * (1 + NVL(v_trx.tasa_interes, 0)));

      -- Determina el tramo SBIF según monto total calculado.
      SELECT porc_aporte_sbif
      INTO v_porcentaje_sbif
      FROM tramo_aporte_sbif
      WHERE v_monto_total
            BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;

      -- Aporte SBIF: porcentaje del monto total.
      v_aporte_sbif :=
         ROUND(v_monto_total * v_porcentaje_sbif / 100);

      -- Inserta el detalle calculado para trazabilidad y posterior resumen.
      INSERT INTO detalle_aporte_sbif
      VALUES (
         v_trx.numrun,
         v_trx.dvrun,
         v_trx.nro_tarjeta,
         v_trx.nro_transaccion,
         v_trx.fecha_transaccion,
         v_trx.tipo_transaccion,
         v_monto_total,
         v_aporte_sbif
      );

      v_procesados := v_procesados + 1;
   END LOOP;
   CLOSE c_detalle;

      -- Validación: si no hay registros, se dispara excepción de negocio.
      IF v_total_registros = 0 THEN
         RAISE e_sin_transacciones;
      END IF;

      -- Validación: el conteo procesado debe coincidir con el conteo esperado.
      IF v_procesados <> v_total_registros THEN
         RAISE e_diferencia_conteo;
      END IF;

   /* ===============================
         RESUMEN (INSERT ORDENADO + CONTROL)
      =============================== */
      SELECT COUNT(*)
        INTO v_total_resumen
        FROM (
                      -- Total de combinaciones (mes,año) y tipo que deben resumirse.
               SELECT DISTINCT TO_CHAR(fecha_transaccion, 'YYYYMM') AS yyyymm,
                               tipo_transaccion
                 FROM detalle_aporte_sbif
             );

      v_res_insertados := 0;

      FOR r_mes IN (
         SELECT DISTINCT
                TO_CHAR(fecha_transaccion, 'YYYYMM') AS yyyymm,
                TO_CHAR(fecha_transaccion, 'MMYYYY') AS mes_anno
           FROM detalle_aporte_sbif
          ORDER BY TO_CHAR(fecha_transaccion, 'YYYYMM')
      ) LOOP
         -- Cursor por mes (YYYYMM) para acumular montos por tipo en ese mes.
         OPEN c_resumen(r_mes.yyyymm);

         v_res_tipo_actual := NULL;
         v_res_monto_acum := 0;
         v_res_aporte_acum := 0;

         LOOP
            FETCH c_resumen INTO v_res_tipo, v_res_monto_det, v_res_aporte_det;
            EXIT WHEN c_resumen%NOTFOUND;

            IF v_res_tipo_actual IS NULL THEN
               v_res_tipo_actual := v_res_tipo;
            END IF;

            IF v_res_tipo <> v_res_tipo_actual THEN
               -- Cambio de tipo: guarda el acumulado del tipo anterior.
               INSERT INTO resumen_aporte_sbif
               VALUES (r_mes.mes_anno,
                       v_res_tipo_actual,
                       v_res_monto_acum,
                       v_res_aporte_acum);
               v_res_insertados := v_res_insertados + 1;

               v_res_tipo_actual := v_res_tipo;
               v_res_monto_acum := 0;
               v_res_aporte_acum := 0;
            END IF;

            v_res_monto_acum := v_res_monto_acum + v_res_monto_det;
            v_res_aporte_acum := v_res_aporte_acum + v_res_aporte_det;
         END LOOP;

         IF v_res_tipo_actual IS NOT NULL THEN
            -- Inserta el último tipo del mes (el loop inserta cuando detecta cambio).
            INSERT INTO resumen_aporte_sbif
            VALUES (r_mes.mes_anno,
                    v_res_tipo_actual,
                    v_res_monto_acum,
                    v_res_aporte_acum);
            v_res_insertados := v_res_insertados + 1;
         END IF;

         CLOSE c_resumen;
      END LOOP;

      IF v_res_insertados <> v_total_resumen THEN
         -- Validación final: se insertan exactamente todos los resúmenes esperados.
         RAISE e_diferencia_conteo;
      END IF;

      -- Confirma todo el proceso si no hubo errores.
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('COMMIT OK: Proceso finalizado correctamente.');

   DBMS_OUTPUT.PUT_LINE('Total esperados : ' || v_total_registros);
   DBMS_OUTPUT.PUT_LINE('Total procesados: ' || v_procesados);
   DBMS_OUTPUT.PUT_LINE('Resumen esperados: ' || v_total_resumen);
   DBMS_OUTPUT.PUT_LINE('Resumen insertados: ' || v_res_insertados);

EXCEPTION
   WHEN e_sin_transacciones THEN
   -- Error de negocio: no se procesa nada, se registra en log.
      ROLLBACK TO sp_proceso;
      log_error(
         'e_sin_transacciones: No existen Avances/Súper Avances para el año ' || v_anno_proceso,
         -20001,
         'No se encontraron transacciones para el periodo'
      );
      DBMS_OUTPUT.PUT_LINE('ERROR: no existen transacciones para el año ' || v_anno_proceso);

   WHEN e_diferencia_conteo THEN
      -- Error de control: diferencias entre esperados y procesados/insertados.
      ROLLBACK TO sp_proceso;
      log_error(
         'e_diferencia_conteo: detalle esp/proc=' || v_total_registros || '/' || v_procesados ||
         ', resumen esp/ins=' || v_total_resumen || '/' || v_res_insertados,
         -20002,
         'Total esperado no coincide con procesados'
      );
      DBMS_OUTPUT.PUT_LINE('ERROR: diferencia entre registros esperados y procesados.');

   WHEN e_invalid_number THEN
      -- Error típico por conversión implícita a número.
      ROLLBACK TO sp_proceso;
      log_error('e_invalid_number (ORA-01722)', SQLCODE, SQLERRM);
      DBMS_OUTPUT.PUT_LINE('ERROR: invalid number (ORA-01722).');

   WHEN NO_DATA_FOUND THEN
      -- Puede ocurrir si no existe tramo SBIF para el monto calculado.
      ROLLBACK TO sp_proceso;
      log_error('NO_DATA_FOUND: sin tramo SBIF o datos faltantes', SQLCODE, SQLERRM);
      DBMS_OUTPUT.PUT_LINE('ERROR: no se encontró tramo SBIF (NO_DATA_FOUND).');

   WHEN OTHERS THEN
      -- Cualquier otro error inesperado.
      ROLLBACK TO sp_proceso;
      log_error('OTHERS: error no controlado', SQLCODE, SQLERRM);
      DBMS_OUTPUT.PUT_LINE('ERROR NO CONTROLADO: ' || SQLERRM);

END;
/

-- Consulta de verificación: detalle generado.
SELECT * FROM DETALLE_APORTE_SBIF
ORDER BY fecha_transaccion, numrun;

-- Consulta de verificación: resumen por mes y tipo.
SELECT * FROM RESUMEN_APORTE_SBIF
ORDER BY TO_DATE(mes_anno, 'MMYYYY'), tipo_transaccion;

-- Consulta de verificación: errores registrados durante la ejecución.
SELECT * FROM ERROR_LOG
ORDER BY fecha_error;
