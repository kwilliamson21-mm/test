SELECT --*,
       timeout.fact_integrated_natural_key_hash_uuid,
       timeout.dim_employee_natural_key_hash_uuid,
       timeout.short_dt,
       employee.party_employee_id,
       employee.mmid,
       employee.team_id,
       employee.organization_id,
       employee.department_id,
       role_id,
       role_nm,
       employee_last_nm,
       employee_first_nm,
       manager_mmid,
       manager_last_nm,
       manager_first_nm,
       working_hours,
       admin_time,
       prod_credit,
       prod_goal,
       non_prod_goal,
       actual_flex_hrs,
       actual_non_prod_hrs,
       actual_ooo_hrs,
       actual_ot_hrs,
       actual_prod_hrs,
       planned_flex_hrs,
       planned_non_prod_hrs,
       planned_ooo_hrs,
       planned_ot_hrs,
       planned_prod_hrs,
       planned_excused_hrs,
       actual_excused_hrs,
       actual_makeup_hrs,
       planned_makeup_hrs,
       all_day_ooo,
       CASE
           WHEN (working_hours - admin_time - planned_non_prod_hrs - planned_prod_hrs - planned_excused_hrs -
                 planned_ooo_hrs + planned_ot_hrs) < 0 THEN 0
           ELSE (working_hours - admin_time - planned_non_prod_hrs - planned_prod_hrs - planned_excused_hrs -
                 planned_ooo_hrs + planned_ot_hrs)
           END                    AS hours_available,
       hours_available * 60 AS prod_credits_available,
       complexity_level_limit
FROM dma_vw.fact_aggregated_performance_vw timeout
         JOIN dma_vw.dma_dim_employee_curr_vw employee
              ON timeout.dim_employee_natural_key_hash_uuid = employee.dim_employee_natural_key_hash_uuid AND
                 employee.team_id IN (45, 48) AND employee.active_ind = TRUE AND
                 employee.end_dt > CURRENT_DATE
         JOIN dma_analytics.lc_examiner_levels levels
              ON employee.party_employee_id = levels.party_employee_id
WHERE timeout.short_dt = '2024-05-10'
  AND timeout.is_weekday = TRUE
  AND timeout.is_holiday = FALSE