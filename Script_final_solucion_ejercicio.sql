/*===========================================================================
ALUMNOS:
- ALONSO BASUALDO
- GUSTAO DOMINGUEZ

SOLUCIONES INFORMATICAS SOLUINFO

DESC: PROCEDIMIENTO AUXILIAR ENCARGADO DE INSERTAR DATOS EN LA TABLA 
        'GASTO_COMUN_PAGO_CERO'
OBJ: MODULARIZAR LA INSERCION DE DATOS PARA MANTENER EL CODIGO LIMPIO
===========================================================================*/

CREATE OR REPLACE PROCEDURE PRC_LLENA_REPORTE_MOROSOS (
    p_anno_mes      IN NUMBER,
    p_id_edif       IN NUMBER,
    p_nom_edificio  IN VARCHAR2,
    p_run_admin     IN VARCHAR2,
    p_nom_admin     IN VARCHAR2,
    p_nro_depto     IN NUMBER,
    p_run_resp      IN VARCHAR2,
    p_nom_resp      IN VARCHAR2,
    p_valor_multa   IN NUMBER,
    p_observacion   IN VARCHAR2
) IS
BEGIN
    INSERT INTO GASTO_COMUN_PAGO_CERO(
        ANNO_MES_PCGC,
        ID_EDIF,
        NOMBRE_EDIF,
        RUN_ADMINISTRADOR,
        NOMBRE_ADMNISTRADOR,
        NRO_DEPTO,
        RUN_RESPONSABLE_PAGO_GC,
        NOMBRE_RESPONSABLE_PAGO_GC,
        VALOR_MULTA_PAGO_CERO,
        OBSERVACION
    ) VALUES (
        p_anno_mes,
        p_id_edif,
        p_nom_edificio,
        p_run_admin,
        p_nom_admin,
        p_nro_depto,
        p_run_resp,
        p_nom_resp,
        p_valor_multa,
        p_observacion
    );

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR AL INSERTAR REPORTE: ' || SQLERRM);
        RAISE;
END;
/

/*PROCEDIMIENTO PRINCIPAL DE LOGICA DE NEGOCIO
OBJ: IDENTIFICAR DEPARTAMENTOS QUE NO PAGARON EL GASTO COMUN ANTERIOR
CALCULAR MULTAS SEGUN REINCIDENCIA
ACTUALIZAR LA MULTA EN LA TABLA GASTO_COMUN
LLAMAR AL PROCEDIMIENTO AUXILIAR CREADO ANTERIORMENTE PARA GENERAR EL REPORTE*/

CREATE OR REPLACE PROCEDURE PRC_PROCESO_MASIVO_MULTAS (
    p_valor_uf  IN NUMBER,
    p_fecha_proceso IN DATE,
    p_cantidad_multados OUT NUMBER,
    p_monto_total_cobrado OUT NUMBER
)IS
    --VARIABLES PARA CONTROL DE FECHAS FORMATO NUMERICO YYYYMM
    --SE UTILIZA NUMBER(6) PARA COINCIDIR CON LA DEFINICION DE LA TABLA
    v_periodo_actual    NUMBER(6);
    v_periodo_anterior  NUMBER(6);
    v_periodo_tras_ant  NUMBER(6); 
    
    --  VARIABLES PARA LOGICA DE CALCULOS
    v_pagos_periodo_anterior      NUMBER;
    v_pagos_periodo_tras_anterior NUMBER;
    v_multa_uf                    NUMBER;
    v_multa_pesos                 NUMBER;
    v_observacion_texto           VARCHAR2(200);
    
    --CURSOR EXPLICITO QUE TRAE TODOS LOS GASTOS COMUNES DEL PERIODO ACTUAL JUNTO CON 
    --LA INFORMACION DE EDIFICIOS, ADMINISTRADORES Y RESPONSABLES PARA EL REPORTE
    --MEJORA: se parametriza el periodo para evitar dependencias implícitas y mejorar claridad
    CURSOR c_proceso(p_periodo NUMBER) IS
        SELECT
            gc.ANNO_MES_PCGC,
            gc.ID_EDIF,
            gc.NRO_DEPTO,
            gc.fecha_pago_gc,
            ed.NOMBRE_EDIF,
            --FORMATEO DE RUT ADMINISTRADOR
            TRIM(TO_CHAR(ad.NUMRUN_ADM, '99G999G999', 'NLS_NUMERIC_CHARACTERS='',.''')) || '-' || ad.DVRUN_ADM AS RUN_ADMIN,
            --FORMATEO DE NOMBRE ADMINISTRADOR
            INITCAP(ad.PNOMBRE_ADM || ' ' || ad.APPATERNO_ADM || ' ' || ad.APMATERNO_ADM) AS NOM_ADMIN,
            --FORMATEO DE RUT RESPONSABLE
            TRIM(TO_CHAR(rp.NUMRUN_RPGC, '99G999G999', 'NLS_NUMERIC_CHARACTERS='',.''')) || '-' || rp.DVRUN_RPGC AS RUN_RESP,
            --FORMATEO DE NOMBRE RESPONSABLE
            INITCAP(rp.PNOMBRE_RPGC || ' ' || rp.APPATERNO_RPGC || ' ' || rp.APMATERNO_RPGC) AS NOM_RESP
        FROM GASTO_COMUN GC
        --CRUCE CON TABLA EDIFICIO PARA OBTENER EL NOMBRE DEL EDIFICIO
        JOIN EDIFICIO ed ON gc.id_edif = ed.id_edif
        --CRUCE CON ADMINISTRADOR PARA OBTENER LOS DATOS DEL ADMIN
        JOIN ADMINISTRADOR ad ON ed.numrun_adm = ad.numrun_adm
        --CRUCE DIRECTO CON RESPONSABLE
        JOIN RESPONSABLE_PAGO_GASTO_COMUN rp ON gc.numrun_rpgc = rp.numrun_rpgc
        WHERE gc.anno_mes_pcgc = p_periodo
        ORDER BY ed.nombre_edif ASC, gc.nro_depto ASC;
    
    -- VARIABLE TIPO REGISTRO QUE ALMACENA UNA FILA COMPLETA DEL CURSOR
    v_reg c_proceso%ROWTYPE;

BEGIN
    --ZONA DE EJECUCION
    --1.- LIMPIEZA DE TABLAS
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GASTO_COMUN_PAGO_CERO';
    
    p_cantidad_multados := 0;
    p_monto_total_cobrado := 0;
    
    --2.- CALCULO DE PERIODOS
    -- SE TRANSFORMA FECHA INGRESADA A FORMATO NUMERICO(6)
    v_periodo_actual    := TO_NUMBER(TO_CHAR(p_fecha_proceso, 'YYYYMM'));
    v_periodo_anterior  := TO_NUMBER(TO_CHAR(ADD_MONTHS(p_fecha_proceso, -1), 'YYYYMM'));
    v_periodo_tras_ant  := TO_NUMBER(TO_CHAR(ADD_MONTHS(p_fecha_proceso, -2), 'YYYYMM'));
    
    
    DBMS_OUTPUT.PUT_LINE('INICIANDO PROCESO DE MULTAS');
    DBMS_OUTPUT.PUT_LINE('PERIODO DEL PROCESO: ' || v_periodo_actual);
    DBMS_OUTPUT.PUT_LINE('PERIODO DE CONTROL DE PAGO_: ' || v_periodo_anterior);
    
    --3.- CICLO DE PROCESAMIENTO, RECORRIDO DEL CURSOR
    OPEN c_proceso(v_periodo_actual);
    LOOP
        FETCH c_proceso INTO v_reg;
        EXIT WHEN c_proceso%NOTFOUND;
        
        --VERIFICAMOS LA MOROSIDAD Y CONTAMOS SI EXISTE UN REGISTRO EN LA TABLA DE PAGOS PARA EL MES ANTERIOR
        SELECT COUNT(*) INTO v_pagos_periodo_anterior
        FROM pago_gasto_comun
        WHERE anno_mes_pcgc = v_periodo_anterior
            AND id_edif = v_reg.id_edif
            AND nro_depto = v_reg.nro_depto;
        
        --TOMAMOS LAS DECICIONES DE LOS CASOS
        IF v_pagos_periodo_anterior = 0 THEN
            --CASO 1:  EL DEPARTAMENTO ES NO PAGO O MOROSO Y SE REVISA SI TAMPOCO PAGO HACE DOS MESES
            SELECT COUNT(*) INTO v_pagos_periodo_tras_anterior
            FROM pago_gasto_comun
            WHERE anno_mes_pcgc = v_periodo_tras_ant
                AND id_edif = v_reg.id_edif
                AND nro_depto = v_reg.nro_depto;
                
            --CALCULO DEL MONTO DE LA MULTA EN UF
            IF v_pagos_periodo_tras_anterior = 0 THEN
                v_multa_uf := 4; -- NO PAGO EL ANTERIOR NI EL TRAS-ANTERIOR
                v_observacion_texto := 'Se realizara el corte del combustible y agua';
            ELSE
                v_multa_uf := 2;  --SOLO DEBE EL MES ANTERIOR
                v_observacion_texto := 'Se realizara el corte del combustible y agua a contar del ' 
                                        || TO_CHAR(v_reg.fecha_pago_gc, 'DD/MM/YYYY');
            END IF;
            
            --CONVERSION DE UF A PESOS
            v_multa_pesos := ROUND(v_multa_uf * p_valor_uf);
            
            p_cantidad_multados := NVL(p_cantidad_multados,0) + 1;
            p_monto_total_cobrado := NVL(p_monto_total_cobrado, 0 ) + v_multa_pesos;
            
            --ACTUALIZACION DE LA DEUDA Y SE ACTUALIZA LA COLUMNA MULTA_GASTO_COMUN DE LA TABLA MAESTRA 
            UPDATE GASTO_COMUN
            SET MULTA_GC = v_multa_pesos
            WHERE anno_mes_pcgc = v_periodo_actual
                AND id_edif = v_reg.id_edif
                AND nro_depto = v_reg.nro_depto;
                
            --GENERACION DEL REPORTE
            --SE LLAMA AL PROCEDIMIENTO AUX PARA GUARDAR EN LA TABLA DE SALIDA
            PRC_LLENA_REPORTE_MOROSOS(
                v_periodo_actual,
                v_reg.id_edif,
                v_reg.nombre_edif,
                v_reg.run_admin,
                v_reg.nom_admin,
                v_reg.nro_depto,
                v_reg.run_resp,
                v_reg.nom_resp,
                v_multa_pesos,
                v_observacion_texto
            );
        ELSE
            --CASO: EL DEPARTAMENTO ESTA AL DIA, ASEGURANDO QUE LA MULTA SEA CERO
            UPDATE gasto_comun
            SET multa_gc = 0
            WHERE anno_mes_pcgc = v_periodo_actual
                AND id_edif = v_reg.id_edif
                AND nro_depto = v_reg.nro_depto;
        END IF;
    END LOOP;
    CLOSE c_proceso;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('PROCESO FINALIZADO CORRECTAMENTE');

EXCEPTION
    --MANEJO DE ERRORES GENERALES
    WHEN OTHERS THEN
        ROLLBACK; -- DESHACEMOS CAMBIOS EN CASO DE ERROR
        DBMS_OUTPUT.PUT_LINE('ERROR CRITICO EN PRC_PROCESO_MASIVO_MULTAS: ' || SQLERRM);
END;
/





/*==========================PRUEBAS==============================*/

SET SERVEROUTPUT ON;

DECLARE
    -- VARIABLES DE ENTRADA (IN)
    v_uf_in    NUMBER := 29509;
    v_fecha_in DATE   := TO_DATE('31/05/' || TO_CHAR(SYSDATE, 'YYYY'), 'DD/MM/YYYY');

    -- VARIABLES DE SALIDA (OUT) - ¡ESTOS SON LOS NOMBRES QUE MANDAN!
    v_resultado_cantidad NUMBER;
    v_resultado_monto    NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('--- INICIO PRUEBA ---');

    -- LLAMADA AL PROCEDIMIENTO
    -- Fíjate que paso las variables declaradas arriba
    PRC_PROCESO_MASIVO_MULTAS(
        v_uf_in,
        v_fecha_in,
        v_resultado_cantidad, -- Recibe el conteo
        v_resultado_monto     -- Recibe la suma
    );

    -- IMPRESION DE RESULTADOS
    -- Uso EXACTAMENTE los mismos nombres declarados arriba
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    DBMS_OUTPUT.PUT_LINE('RESUMEN FINAL:');
    DBMS_OUTPUT.PUT_LINE('Cantidad de Multados : ' || NVL(v_resultado_cantidad, 0));
    DBMS_OUTPUT.PUT_LINE('Monto Total en $     : ' || NVL(v_resultado_monto, 0));
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    
    DBMS_OUTPUT.PUT_LINE('--- FIN PRUEBA ---');
END;
/
    
    
SELECT 
    ANNO_MES_PCGC,
    ID_EDIF,
    NOMBRE_EDIF,
    RUN_ADMINISTRADOR,
    NOMBRE_ADMNISTRADOR,
    NRO_DEPTO,
    RUN_RESPONSABLE_PAGO_GC,
    NOMBRE_RESPONSABLE_PAGO_GC,
    OBSERVACION
FROM GASTO_COMUN_PAGO_CERO
ORDER BY NOMBRE_EDIF, NRO_DEPTO;

SELECT 
    ANNO_MES_PCGC, 
    ID_EDIF, 
    NRO_DEPTO, 
    FECHA_DESDE_GC,  
    FECHA_HASTA_GC, 
    MULTA_GC        
FROM GASTO_COMUN
WHERE ANNO_MES_PCGC = 202605  -- FILTRA SOLO EL MES ACTUAL (Mayo)
  AND MULTA_GC > 0            -- FILTRAMOS SOLO LOS QUE TENGAN MULTAS(Pago Cero)
ORDER BY ID_EDIF, NRO_DEPTO;