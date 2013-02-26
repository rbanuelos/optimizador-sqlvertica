/* pega claves de pendientes de día anterior */

      INSERT INTO STAGING.FCT_TRAFICO_GPRS_TEMPORAL --C
      SELECT a.apn_id, a.fecha_aux, a.plan_gprs_id, a.plan_tarifario_gprs_id,
             a.clase_credito_gprs_id,
             a.tipo_descuento_id,
             a.monto_credito_descontado, a.monto_tasado, a.kb_descontado,
             a.kb_cobrado, a.kb_tasado, a.datos_bajada, a.datos_subida,
             a.conexiones, a.duracion_segundos, a.numero_linea, LC.ALM_KEY AS alm_key,
              (CASE
                  WHEN a.APN_KEY IS NULL
                  THEN
                     (SELECT V.APN_KEY
                        FROM STAGING.V_APN_ACTUAL V
                       WHERE V.APN_ID = a.APN_ID)
                  ELSE
                     a.APN_KEY
               END) AS apn_key,
             a.fecha_key, a.hora_key, LC.PLAN_LLAMADA_ACT_KEY AS plan_llamada_key,
             LC.PLAN_TARIFARIO_LLAMADA_ACT_KEY AS plan_tarifario_llamada_key, LC.CLASE_CREDITO_LLAMADA_ACT_KEY AS clase_credito_llamada_key,
             LC.PLAN_MENSAJE_ACT_KEY AS plan_mensaje_key, LC.PLAN_TARIFARIO_MENSAJE_ACT_KEY AS plan_tarifario_mensaje_key,
             LC.CLASE_CREDITO_MENSAJE_ACT_KEY AS clase_credito_mensaje_key,
              (CASE
                  WHEN a.PLAN_GPRS_KEY IS NULL
                  THEN
                     (SELECT V.PLAN_KEY
                        FROM STAGING.V_PLAN_GPRS_ACTUAL V
                       WHERE V.PLAN_ACTUAL_ID = a.PLAN_GPRS_ID)
                  ELSE
                     a.PLAN_GPRS_KEY
               END) AS plan_gprs_key,
             LC.PLAN_TARIFARIO_GPRS_KEY AS plan_tarifario_gprs_key,
              (CASE
                  WHEN a.CLASE_CREDITO_GPRS_KEY IS NULL
                  THEN
                     (SELECT V.CLASE_CREDITO_KEY
                        FROM STAGING.V_CLASE_CREDITO_ACTUAL V
                       WHERE V.CLASE_CREDITO_ID = a.CLASE_CREDITO_GPRS_ID)
                  ELSE
                     a.CLASE_CREDITO_GPRS_KEY
               END) clase_credito_gprs_key,
              (CASE
                  WHEN a.TIPO_DESCUENTO_KEY IS NULL
                  THEN
                     (SELECT V.TIPO_DESCUENTO_KEY
                        FROM STAGING.V_TIPO_DESCUENTO_ACTUAL V
                       WHERE V.TIPO_DESCUENTO_ID = a.TIPO_DESCUENTO_ID)
                  ELSE
                     a.TIPO_DESCUENTO_KEY
               END) AS tipo_descuento_key,
             LC.TECNOLOGIA_ACT_KEY AS tecnologia_key, LC.LOCALIDAD_TITULAR_KEY AS localidad_key,
             LC.ESTADO_LINEA_ACT_KEY AS estado_linea_key, a.cantidad_veces, a.insertado,
             a.mb_descontado, a.mb_cobrado, a.mb_tasado, a.duracion_minutos,
             a.fecha_tasacion_key, a.hora_tasacion_key,
             a.monto_credito_descontado_usd, a.monto_tasado_usd, a.celda_id,
              (CASE
                  WHEN a.CELDA_KEY IS NULL
                  THEN
                     (SELECT V.CELDA_KEY
                        FROM STAGING.V_CELDA_ACTUAL V
                       WHERE V.CELDA_ID = a.CELDA_ID)
                  ELSE
                     a.CELDA_KEY
               END) AS celda_key,
             LC.TIPO_CLIENTE_KEY AS tipo_persona_key, LC.TIPO_SOCIEDAD_KEY AS tipo_sociedad_key,
             
             TO_NUMBER(LC.cliente_titular) AS codigo_cliente, a.charging_id, a.es_roaming_id, -- cambiar el tipo de codigo_cliente en el DDL
             
             a.operadora_roaming_id,
              (CASE
                  WHEN a.ES_ROAMING_KEY IS NULL
                  THEN
                     (SELECT T.ES_ROAMING_KEY
                        FROM DWH.ES_ROAMING t
                       WHERE T.CODIGO = a.es_roaming_id)
                  ELSE
                     a.ES_ROAMING_KEY
               END) AS es_roaming_key,
              (CASE
                  WHEN a.OPERADORA_ROAMING_KEY IS NULL
                  THEN
                     (SELECT s.operadora_roa_key
                        FROM staging.v_operadora_roa_actual s
                       WHERE s.operadora_roa_id = a.operadora_roaming_id)
                  ELSE
                     a.OPERADORA_ROAMING_KEY
               END) AS operadora_roaming_key,
             a.duracion_tasada, a.kb_traficado,
             a.excedente_credito,  -- ESTO ES PARA EL 15 DE FEBRERO
             a.saldo_credito_actual,
             a.saldo_datos_actual,
              (CASE
                  WHEN a.TECNOLOGIA_ACCESO_KEY IS NULL
                  THEN
                     (SELECT V.TECNOLOGIA_ACCESO_KEY
                        FROM STAGING.V_TECNOLOGIA_ACCESO_ACTUAL V
                       WHERE V.TECNOLOGIA_ACCESO_ID = a.TECNOLOGIA_ACCESO_ID)
                  ELSE
                     a.TECNOLOGIA_ACCESO_KEY
               END) AS tecnologia_acceso_key,
             a.tecnologia_acceso_id, a.cantidad_servicios, a.fecha_tasacion,
             a.record_sequence_number, a.local_record_sequence_number,
             a.mto_iva, a.mto_total, a.node_id, a.fraccionamiento,
             a.rating_group,
              (CASE
                  WHEN a.RATING_GROUP_KEY IS NULL
                  THEN
                      (SELECT r.rating_group_key
                      FROM staging.v_rating_group_actual r
                      WHERE r.rating_group_ID = a.RATING_GROUP)
                  ELSE
                     a.RATING_GROUP_KEY
               END) AS rating_group_key,
             a.service_identifier,
              (CASE
                  WHEN a.service_identifier_key IS NULL
                  THEN
                      CASE
                        WHEN a.RATING_GROUP = 0 AND a.SERVICE_IDENTIFIER IS NULL THEN
                           -1
                        WHEN a.RATING_GROUP = 1 AND a.apn_id = 'INTERNETBA' THEN
                           -1
                      ELSE
                          (SELECT s.service_identifier_key
                             FROM staging.v_service_identifier_actual s
                            WHERE s.service_identifier_ID = a.SERVICE_IDENTIFIER)
                      END
                  ELSE
                     a.service_identifier_key
               END) AS service_identifier_key,
             a.metodo_cobro,
              (CASE
                  WHEN a.metodo_cobro_key IS NULL
                  THEN
                      (SELECT T.METODO_COBRO_KEY
                      FROM DWH.METODO_COBRO t
                      WHERE T.codigo = a.METODO_COBRO)
                  ELSE
                     a.metodo_cobro_key
               END) AS metodo_cobro_key,
             a.es_tasacion_flat,
              (CASE
                  WHEN a.es_tasacion_flat_key IS NULL
                  THEN
                      (SELECT T.es_tasacion_flat_KEY
                      FROM DWH.es_tasacion_flat t
                      WHERE T.codigo = a.es_tasacion_flat)
                  ELSE
                     a.es_tasacion_flat_key
               END) AS es_tasacion_flat_key, a.cod_tar_gprs,
              (CASE
                  WHEN a.cod_tar_gprs_key IS NULL
                  THEN
                       (SELECT T.tarifa_gprs_KEY
                      FROM DWH.tarifa_gprs t
                      WHERE T.codigo = a.COD_TAR_GPRS)
                  ELSE
                     a.cod_tar_gprs_key
               END) AS cod_tar_gprs_key,
             LC.CELDA_X_ZONA_KEY AS celda_x_zona_key,
             a.TRAFICO_PACK,
             a.CANTIDAD,
             a.SERVICIO_TASABLE_ID,
             a.DESCUENTO_ID,
             a.SERVICIO_COMERCIAL_ID,
             a.SERVICIO_PACK_ID,
             a.PAQUETE_SERVICIO_ID,
             (CASE
                WHEN a.SERVICIO_TASABLE_KEY IS NULL
                THEN
                    (SELECT T.servicio_tasable_key
                     FROM staging.v_servicio_tasable_actual t
                     WHERE t.servicio_tasable_id = a.SERVICIO_TASABLE_ID)
                ELSE
                    a.SERVICIO_TASABLE_KEY
              END) AS SERVICIO_TASABLE_KEY,
              (CASE
                 WHEN a.DESCUENTO_KEY IS NULL
                 THEN
                   (SELECT T.descuento_key
                     FROM staging.v_descuento_actual t
                     WHERE t.descuento_id = a.DESCUENTO_ID)
                  ELSE
                     a.DESCUENTO_KEY
               END) AS DESCUENTO_KEY,
              (CASE
                  WHEN a.CANTIDAD_SERV_APLICADOS_KEY IS NULL
                  THEN
                       (SELECT T.rango_cantidad_servicios_key
                        FROM DWH.rango_cantidad_servicios t
                        WHERE t.codigo = a.CANTIDAD_SERVICIOS)
                  ELSE
                     a.CANTIDAD_SERV_APLICADOS_KEY
               END) AS CANTIDAD_SERV_APLICADOS_KEY,
               a.APLICA_SERVICIOS_KEY,
               (CASE
                  WHEN a.SERVICIO_COMERCIAL_KEY IS NULL
                  THEN
                       (SELECT T.rango_cantidad_servicios_key
                        FROM DWH.rango_cantidad_servicios t
                        WHERE t.codigo = a.SERVICIO_COMERCIAL_ID)
                  ELSE
                     a.SERVICIO_COMERCIAL_KEY
                END) AS SERVICIO_COMERCIAL_KEY,
               (CASE
                  WHEN a.SERVICIO_PACK_KEY IS NULL
                  THEN
                       (SELECT T.servicio_pack_key
                        FROM STAGING.V_SERVICIO_PACK_ACTUAL t
                        WHERE t.SERVICIO_PACK_ID = NVL(a.SERVICIO_PACK_ID,'-1'))
                  ELSE
                     a.SERVICIO_PACK_KEY
                END) AS SERVICIO_PACK_KEY,
                (CASE
                  WHEN a.PAQUETE_SERVICIO_KEY IS NULL
                  THEN
                       (SELECT t.PAQUETE_SERVICIO_KEY
                        FROM STAGING.V_PAQUETE_SERVICIO_ACTUAL t
                        WHERE t.PAQUETE_SERVICIO_ID = NVL(a.PAQUETE_SERVICIO_ID, '-1'))
                  ELSE
                     a.PAQUETE_SERVICIO_KEY
                END) AS PAQUETE_SERVICIO_KEY,
                a.DESCONTADO_DATOS,
                a.DESCONTADO_CREDITO,
                a.GS_ABONO_CARGA,
                a.USD_ABONO_CARGA,
                a.EXCEDENTE_CREDITO_USD,
                a.MONTO_DATOS_DESC,
                a.TIPO_CREDITO_DESCONTADO_KEY,
                a.TRAFICO_SIN_COSTO,
                a.TRAFICO_CON_COSTO,
                a.GPRS_CON_COSTO_KEY,
                (CASE
                  WHEN a.MARCA_VIP_KEY IS NULL
                  THEN
                       (SELECT T.marca_vip_key_key
                           FROM staging.v_marca_vip_actual T
                           WHERE T.marca_vip_key_id = NVL(LC.segmento_empresa,'-1'))
                  ELSE
                     a.MARCA_VIP_KEY
                END) AS MARCA_VIP_KEY,
                NVL(lc.segmento_linea_key, -2) AS segmento_linea_key,
                NVL(lc.segmento_cliente_key, -2) AS segmento_cliente_key,
                a.CARACTERISTICA_ABONADO_ID,
                a.MANEJO_CONTINUIDAD_FALLO_ID,
                (CASE
                    WHEN a.CARACTERISTICA_ABONADO_KEY IS NULL
                    THEN
                        (SELECT t.caracteristica_abonado_key
                               FROM dwh.caracteristica_abonado t
                               WHERE t.codigo = a.CARACTERISTICA_ABONADO_ID)
                    ELSE
                        a.CARACTERISTICA_ABONADO_KEY
                END) AS CARACTERISTICA_ABONADO_KEY,
                (CASE
                    WHEN a.MANEJO_CONTINUIDAD_FALLO_KEY IS NULL
                    THEN
                        (SELECT t.manejo_continuidad_fallo_key
                               FROM dwh.manejo_continuidad_fallo t
                               WHERE t.codigo = a.MANEJO_CONTINUIDAD_FALLO_ID)
                    ELSE
                        a.MANEJO_CONTINUIDAD_FALLO_KEY
                END) AS MANEJO_CONTINUIDAD_FALLO_KEY,
                CANTIDAD_HILO,
                NVL(lc.SEGMENTO_INTERNET_KEY, -2) AS SEGMENTO_INTERNET_KEY
      FROM STAGING.FCT_TRAFICO_GPRS_PENDIENTE a
      
      LEFT JOIN DWH.V_LINEA_CONTRATO_2 LC
      ON  LC.LINEA = a.NUMERO_LINEA;