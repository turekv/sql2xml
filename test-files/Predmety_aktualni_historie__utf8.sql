/* Historie akt. predmetu

*/
with
    --------------------------- Orgunita - ostra
    pr_o as (select
            pr.zkratka as pr
            , 'Akt. zaznam' as oracle_user, SYSDATE as log_ts, '-' as dml
            , ap.*
            
        from (select * from st01.predmet                                                -- *** Predmet
                       where predmet.status = 9 
                             -- and predmet.fakulta_id = 4
                             -- and predmet.zkratka like '9%' 
                                    ) pr
             inner join st01.aktualni_predmet ap                                        -- *** Aktualni predmet
                   on ( ap.predmet_id = pr.predmet_id
                        -- and ap.status = 9
                        and ap.aktualni_predmet_id in (262216)
--                        and ap.rok = 2020                 -- rok !!! 
--                        and ap.typ_semestru_id = 1      -- 2-ZS, 1-LS
                       )
    ) -- as pr_o     --------------------------- Orgunita - ostra
    
--select tmp.* from pr_o tmp 
--/*

    --------------------------- Ostra + logger
    , pr_all as (
        select o1.*
        from pr_o o1                                                                    -- *** Pr_o
        union all
        select o2.pr, o3.*
        from pr_o o2                                                                    -- *** Pr_o
            inner join logger.aktualni_predmet o3                                       -- *** Aktualni_predmet - logger
                  on o3.aktualni_predmet_id = o2.aktualni_predmet_id
        
    ) -- TESTOVACI KOD PRO SELECT-WHERE V KOMBINACI S INNER JOIN (okolo SELECT-WHERE musi byt zavorky, jinak by zrejme nebylo syntakticky spravne -- INNER JOIN je takto vracen jako soucast WHERE!)
    , pr_all_TEST_WHERE as (
        select o1.*
        from pr_o o1                                                                    -- *** Pr_o
        union all
        ( select o2.pr, o3.*
        from pr_o o2                                                                    -- *** Pr_o
        WHERE pr_o.status = 9     -- PRIDANO PRO POTREBY TESTOVANI 
        )
            inner join logger.aktualni_predmet o3                                       -- *** Aktualni_predmet - logger
                  on o3.aktualni_predmet_id = o2.aktualni_predmet_id
        
    ) 
    
--select tmp.* from pr_all tmp
--/*


select tmp.upd_ts as zmeneno, tmp.upd_uid as zmenil_id
    , pe.jmenovka as zmenil
    , tmp.*
 
from pr_all tmp
    inner join (select pe50.per_id--, pe50.per_family_names as prijmeni, pe50.per_first_names as jmeno 
                 , pe50.label_pr as jmenovka 
                           from brutisadm.person pe50                                    -- *** Person
                           where pe50.status = 9 ) pe
           on pe.per_id = tmp.upd_uid
order by tmp.pr, tmp.aktualni_predmet_id, tmp.upd_ts desc, tmp.log_ts desc
/*

-- */
