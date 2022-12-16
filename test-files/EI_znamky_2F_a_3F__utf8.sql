/* Znamky studentu 2F a 3F, kteri meli ten predmet zapsany v jednom ak. roce
   - tri roky pozpatku
    1.       Počet studentů, kteří mají v témže akademickém roce zapsán předmět 2F a 3F
    2.       Kolik z nich má hodnocení lepší než F v předmětu 3F
    3.       Kolik z nich má hodnocení lepší než F v předmětu 2F
    4.       Kolik z nich má hodnocení lepší než F v obou předmětech

 
   
*/
with
    --------------------------- Predmet 2F a 3F za 3 roky
    pap_tmp as (select pr.zkratka as pr, ap.nazev as predmet
                       , ap.rok , pr.predmet_id, ap.aktualni_predmet_id
        from (select * from st01.predmet where predmet.status = 9                       -- *** Predmet
                                        and predmet.fakulta_id = 4
                                        and predmet.zkratka in ('2F', '3F') 
                                    ) pr
             inner join st01.aktualni_predmet ap                                        -- *** Aktualni predmet
                   on ( ap.predmet_id = pr.predmet_id
                        and ap.status = 9
                        and ap.rok between 2012 and 2015                 -- rok !!! 
                        -- and ap.typ_semestru_id = 1      -- 2-ZS, 1-LS
                       )

    ) -- as pap_tmp     --------------------------- Predmet 2F a 3F za 3 roky
    
--select tmp.* from pap_tmp tmp 
--/*    

    ---------------------------
    , eia_tmp as (select p1.rok
                        , p1.pr as pr1, ei1.znamka_znak as znamka1
                        , p2.pr as pr2, ei2.znamka_znak as znamka2
                        , ei1.studium_id
                        , decode(ei1.znamka_znak, 'F',0, null,0, 1) as lepsi_nez_F_1
                        , decode(ei2.znamka_znak, 'F',0, null,0, 1) as lepsi_nez_F_2
                        , decode(ei1.znamka_znak, 'F',0, null,0, 1)
                          * decode(ei2.znamka_znak, 'F',0, null,0, 1) as lepsi_nez_F_12
        from pap_tmp p1                                                                 -- *** Predmet - aktualni_predmet 2F
            inner join st01.el_index ei1                                                 -- *** El_index 
                  on ( ei1.aktualni_predmet_id = p1.aktualni_predmet_id
                       and p1.pr = '2F' 
                       and ei1.status = 9 )
            inner join st01.pap_tmp p2                                                  -- *** Predmet - aktualni_predmet 2F 
                  on ( p2.pr = '3F'
                       and p2.rok = p1.rok          -- 2F i 3F ve stejnem roce
                      )
            inner join st01.el_index ei2                                                  -- *** El_index 
                  on ( ei2.aktualni_predmet_id = p2.aktualni_predmet_id 
                       and ei2.studium_id = ei1.studium_id  -- Tentyz student
                       and ei2.status = 9 )
    ) -- as eia_tmp     ---------------------------
    
--select tmp.* from eia_tmp tmp
--/*

select eia.rok
       , count(1) studentu_1_2
       , sum(eia.lepsi_nez_f_1) as lepsi_nez_f_1
       , sum(eia.lepsi_nez_f_2) as lepsi_nez_f_2
       , sum(eia.lepsi_nez_f_12) as lepsi_nez_f_12 

from eia_tmp eia
group by eia.rok
order by eia.rok
-- */