SELECT cats.*,
       CASE
           WHEN
               (function_nm IN ('Life Proofs', 'Life 1st Notice', 'Life 2nd Exam', 'Life Follow Ups')
                   OR (function_nm = 'Bene Admin BAU' AND
                       work_event_nm IN ('{LC} SSA First Notice', '{LC} SSA-1st Notice -RMM'))) THEN TRUE
           ELSE FALSE END AS 'target_work_event',
       TRUE               AS 'assigned'
FROM dma_vw.rpt_cats_curr_pend_vw cats
         JOIN dma_vw.dma_dim_employee_curr_vw emp
              ON cats.party_employee_id = emp.party_employee_id AND emp.team_id IN (45, 48) AND
                 emp.active_ind = TRUE AND emp.end_dt > CURRENT_DATE AND
                 emp.party_type_id = 1
UNION ALL
SELECT *,
       CASE
           WHEN
               (function_nm IN ('Life Proofs', 'Life 1st Notice', 'Life 2nd Exam', 'Life Follow Ups')
                   OR (function_nm = 'Bene Admin BAU' AND
                       work_event_nm IN ('{LC} SSA First Notice', '{LC} SSA-1st Notice -RMM'))) THEN TRUE
           ELSE FALSE END AS 'target_work_event',
       FALSE              AS 'assigned'
FROM dma_vw.rpt_cats_curr_pend_vw
WHERE party_employee_id IS NULL
  AND work_event_department_id = 8