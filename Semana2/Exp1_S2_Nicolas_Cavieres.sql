/* ============================================================
   PRY2206 – ACTIVIDAD SUMATIVA 1
   GENERACIÓN DE USUARIOS Y CLAVES

   Descripción general:
   Este proceso PL/SQL recorre todos los empleados de la tabla
   EMPLEADO, genera un nombre de usuario y una clave según reglas
   de negocio definidas, y almacena el resultado en la tabla
   USUARIO_CLAVE, asegurando control transaccional.
   ============================================================ */

SET SERVEROUTPUT ON;

/* ============================================================
   LIMPIEZA PREVIA
   TRUNCATE se utiliza para eliminar todos los registros de la
   tabla destino antes de ejecutar el proceso, garantizando que
   la carga sea completa y consistente.
   ============================================================ */
TRUNCATE TABLE usuario_clave;

/* ============================================================
   VARIABLE BIND PARAMÉTRICA
   Variable externa al bloque PL/SQL utilizada para evitar
   valores "hardcodeados". Representa el mes y año del proceso
   en formato MMYYYY y se usa en la construcción de la clave.
   ============================================================ */
VAR b_anio_proceso VARCHAR2(6);
EXEC :b_anio_proceso := TO_CHAR(SYSDATE,'MMYYYY');

/* ============================================================
   BLOQUE PRINCIPAL PL/SQL
   ============================================================ */
DECLARE
  /* =========================================================
     CURSOR EXPLÍCITO
     Sentencia SQL documentada #1
     Este cursor extrae todos los empleados junto a su estado
     civil, permitiendo separar claramente la fase de
     EXTRACCIÓN de datos de la fase de TRANSFORMACIÓN.
     ========================================================= */
  CURSOR c_emp IS
    SELECT e.id_emp,
           e.numrun_emp,
           e.dvrun_emp,
           e.appaterno_emp,
           e.apmaterno_emp,
           e.pnombre_emp,
           e.snombre_emp,
           e.fecha_nac,
           e.fecha_contrato,
           e.sueldo_base,
           ec.nombre_estado_civil
    FROM empleado e
    JOIN estado_civil ec
      ON e.id_estado_civil = ec.id_estado_civil
    ORDER BY e.id_emp;

  /* =========================================================
     VARIABLES ESCALARES (%TYPE)
     Permiten mantener consistencia con los tipos de datos de
     la base de datos y facilitan el mantenimiento del código.
     ========================================================= */
  v_id_emp           empleado.id_emp%TYPE;
  v_numrun           empleado.numrun_emp%TYPE;
  v_dvrun            empleado.dvrun_emp%TYPE;
  v_appaterno        empleado.appaterno_emp%TYPE;
  v_apmaterno        empleado.apmaterno_emp%TYPE;
  v_pnombre          empleado.pnombre_emp%TYPE;
  v_snombre          empleado.snombre_emp%TYPE;
  v_fecha_nac        empleado.fecha_nac%TYPE;
  v_fecha_contrato   empleado.fecha_contrato%TYPE;
  v_sueldo_base      empleado.sueldo_base%TYPE;
  v_estado_civil     estado_civil.nombre_estado_civil%TYPE;

  /* =========================================================
     VARIABLES DE TRANSFORMACIÓN
     ========================================================= */
  v_nombre_empleado  VARCHAR2(60);
  v_nombre_usuario   VARCHAR2(20);
  v_clave_usuario    VARCHAR2(30);

  v_anios_empresa    NUMBER;
  v_sueldo_menos1    NUMBER;
  v_ultimos3_txt     VARCHAR2(3);
  v_anio_nac2        NUMBER;
  v_dig_run3         VARCHAR2(1);
  v_apellido_txt     VARCHAR2(2);
  v_letra_estado     VARCHAR2(1);

  /* =========================================================
     VARIABLES DE CONTROL TRANSACCIONAL
     ========================================================= */
  v_total_esperado   NUMBER;
  v_total_insertado  NUMBER := 0;

BEGIN
  DBMS_OUTPUT.PUT_LINE('--- INICIO PROCESO USUARIO_CLAVE ---');

  /* =========================================================
     Sentencia SQL documentada #2
     Se obtiene la cantidad total de empleados para validar
     posteriormente que el proceso haya sido exitoso en su
     totalidad antes de realizar COMMIT.
     ========================================================= */
  SELECT COUNT(*)
  INTO v_total_esperado
  FROM empleado;

  DBMS_OUTPUT.PUT_LINE(
    'Registros a procesar desde EMPLEADO: ' || v_total_esperado
  );

  OPEN c_emp;
  LOOP
    FETCH c_emp INTO
      v_id_emp, v_numrun, v_dvrun,
      v_appaterno, v_apmaterno,
      v_pnombre, v_snombre,
      v_fecha_nac, v_fecha_contrato,
      v_sueldo_base, v_estado_civil;

    EXIT WHEN c_emp%NOTFOUND;

    DBMS_OUTPUT.PUT_LINE('-- Ejecutando para usuario id=' || v_id_emp);

    /* =====================================================
       Sentencia PL/SQL documentada #1
       Validación del segundo nombre para informar posibles
       datos incompletos sin detener el proceso.
       ===================================================== */
    IF v_snombre IS NULL THEN
      DBMS_OUTPUT.PUT_LINE(
        '   Aviso: Segundo nombre vacío para id=' || v_id_emp
      );
    END IF;

    /* =====================================================
       Construcción del nombre completo en mayúsculas
       ===================================================== */
    v_nombre_empleado :=
      UPPER(
        v_pnombre || ' ' ||
        NVL(v_snombre || ' ', '') ||
        v_appaterno || ' ' ||
        v_apmaterno
      );

    /* =====================================================
       Sentencia PL/SQL documentada #2
       Cálculo de años de permanencia usando MONTHS_BETWEEN,
       evitando cálculos directos en SQL.
       ===================================================== */
    v_anios_empresa :=
      TRUNC(MONTHS_BETWEEN(SYSDATE, v_fecha_contrato) / 12);

    IF v_anios_empresa < 10 THEN
      DBMS_OUTPUT.PUT_LINE(
        '   Aviso: Empleado con menos de 10 años en la empresa'
      );
    END IF;

    /* =====================================================
       Determinación de letra de estado civil
       ===================================================== */
    IF UPPER(v_estado_civil) = 'CASADO' THEN
      v_letra_estado := 'c';
    ELSIF UPPER(v_estado_civil) = 'ACUERDO UNION CIVIL' THEN
      v_letra_estado := 'a';
    ELSIF UPPER(v_estado_civil) = 'SOLTERO' THEN
      v_letra_estado := 's';
    ELSIF UPPER(v_estado_civil) = 'DIVORCIADO' THEN
      v_letra_estado := 'd';
    ELSIF UPPER(v_estado_civil) = 'VIUDO' THEN
      v_letra_estado := 'v';
    ELSIF UPPER(v_estado_civil) = 'SEPARADO' THEN
      v_letra_estado := 's';
    ELSE
      DBMS_OUTPUT.PUT_LINE(
        '   ERROR: Estado civil no reconocido para id=' || v_id_emp
      );
      v_letra_estado := NULL;
    END IF;

    /* =====================================================
       Construcción del nombre de usuario
       ===================================================== */
    v_nombre_usuario :=
         v_letra_estado
      || UPPER(SUBSTR(v_pnombre,1,3))
      || LENGTH(v_pnombre)
      || '*'
      || SUBSTR(TO_CHAR(v_sueldo_base),-1)
      || v_dvrun
      || v_anios_empresa;

    IF v_anios_empresa < 10 THEN
      v_nombre_usuario := v_nombre_usuario || 'X';
    END IF;

    /* =====================================================
       Construcción de la clave del usuario
       ===================================================== */
    v_dig_run3 := SUBSTR(TO_CHAR(v_numrun),3,1);
    v_anio_nac2 := TO_NUMBER(TO_CHAR(v_fecha_nac,'YYYY')) + 2;

    v_sueldo_menos1 := v_sueldo_base - 1;
    v_ultimos3_txt  := LPAD(MOD(v_sueldo_menos1,1000),3,'0');

    IF UPPER(v_estado_civil) IN ('CASADO','ACUERDO UNION CIVIL') THEN
      v_apellido_txt := LOWER(SUBSTR(v_appaterno,1,2));
    ELSIF UPPER(v_estado_civil) IN ('DIVORCIADO','SOLTERO') THEN
      v_apellido_txt := LOWER(
                          SUBSTR(v_appaterno,1,1) ||
                          SUBSTR(v_appaterno,-1,1)
                        );
    ELSIF UPPER(v_estado_civil) = 'VIUDO' THEN
      v_apellido_txt := LOWER(
                          SUBSTR(v_appaterno,-3,1) ||
                          SUBSTR(v_appaterno,-2,1)
                        );
    ELSIF UPPER(v_estado_civil) = 'SEPARADO' THEN
      v_apellido_txt := LOWER(SUBSTR(v_appaterno,-2,2));
    ELSE
      v_apellido_txt := NULL;
    END IF;

    v_clave_usuario :=
         v_dig_run3
      || v_anio_nac2
      || v_ultimos3_txt
      || v_apellido_txt
      || v_id_emp
      || :b_anio_proceso;

    INSERT INTO usuario_clave
    VALUES (
      v_id_emp,
      v_numrun,
      v_dvrun,
      v_nombre_empleado,
      v_nombre_usuario,
      v_clave_usuario
    );

    v_total_insertado := v_total_insertado + 1;

    DBMS_OUTPUT.PUT_LINE(
      '   Loop ejecutado para usuario id=' || v_id_emp || ' con éxito'
    );

  END LOOP;

  CLOSE c_emp;

  /* =====================================================
     CONTROL TRANSACCIONAL FINAL
     ===================================================== */
  IF v_total_insertado = v_total_esperado THEN
    COMMIT;
    DBMS_OUTPUT.PUT_LINE(
      'COMMIT OK - Registros procesados: ' || v_total_insertado
    );
  ELSE
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE(
      'ROLLBACK - Inconsistencia en el conteo'
    );
  END IF;

  DBMS_OUTPUT.PUT_LINE('--- FIN PROCESO USUARIO_CLAVE ---');

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE(
      'ERROR GENERAL: ' || SQLCODE || ' - ' || SQLERRM
    );
    ROLLBACK;
END;
/

/* =====================================================
  VALIDACION
  ===================================================== */

SELECT * FROM usuario_clave;