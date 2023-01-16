/* Literatura predmetu a zarazeni predmetu v programech
   Kazda literatura je jen 1x, i když se vyskytuje u aktuálních
   předmětů v různých semestrech. V případě více aktuálních předmětů
   (s různými názvy) se vybere některý z nich.
   
   Predmety_literatura_pouziti_v_planech_Apollo.sql

   2022-11-09 - Pridano poradove cislo, lepsi razeni
   2022-11-08 - Bindovane promenne pro vlozeni do Prehledy a statistiky


*/
with
    --------------------------- Vybrane predmety predmety (dle roku akt. predmetu)
    pr_tmp as (select pr.zkratka as pr, ap.nazev as predmet
            -- , ap.aktualni_predmet_id
            , pr.predmet_id
            , ap.rok, org.org_acronym as fak
            , ap.ustav_id
            ---- Kazdy predmet se vezme jen 1x (filtr rn = 1)
            , row_number() over ( partition by pr.predmet_id
                                  order by ap.nazev, ap.aktualni_predmet_id asc) as rn
        from (select * from st01.predmet where predmet.status = 9                       -- *** Predmet
                                        and predmet.fakulta_id = :fakulta_id -- 12-FCH 13-FP   4

                                     -- and predmet.zkratka like '9%'
                                    ) pr
             inner join st01.aktualni_predmet ap                                        -- *** Aktualni predmet
                   on ( ap.predmet_id = pr.predmet_id
                        and ap.status = 9
                        and ap.rok = :akrok                 -- rok !!!
--                        and ap.typ_semestru_id = 1      -- 2-ZS, 1-LS
                       )
            left join brutisadm.orgunit org                                             -- *** Orgunit
                  on org.orgunitid = pr.fakulta_id
            --left join brutisadm.orgunit_binding orb                    -- *** Orgunit_binding - SAP kod
            --      on ( orb.orgunitid = org.orgunitid                   -- code_value
            --           and nvl(orb.valid_to, sysdate) >= sysdate
            --           and orb.code_generator = 'SAP'
            --           and orb.status = 9 )
    ) -- as pr_tmp     --------------------------- Vybrane predmety predmety (dle roku akt. predmetu)

--select tmp.* from pr_tmp tmp
--/*

    --------------------------- Vypis infa o stud. programech zarazenych predmetu
    , pp_tmp as (select ap05.predmet_id
--            , pg.program_id, ob.obor_id
--            , st.stupen_id, ro.rocnik_id, se.semestr_id
            , listagg(distinct ap05.aktualni_predmet_id,', ') within group(order by ap05.predmet_id) as ID_aktualnich_predmetu
            , listagg(distinct cts.zkratka,', ') within group(order by cts.zkratka) as typy
--            , cts.zkratka as typ
--            , cfs.zkratka as forma -- , pg.delka_studia
            , listagg(distinct org.org_acronym,', ') within group(order by org.org_acronym) as fakulty_programu
            , listagg(distinct cfs.zkratka,', ') within group(order by cfs.zkratka) as formy
            -- , st.cislo_stupne as stupen, ro.cislo_rocniku as rocnik, cse.zkratka as semestr
            , listagg(distinct pg.zkratka,', ') within group(order by pg.zkratka) as programy
            , listagg(distinct pg.nazev,'; ') within group(order by pg.nazev) as programy_nazvy
            , listagg(distinct ro.cislo_rocniku,', ') within group(order by ro.cislo_rocniku) as rocniky
            , listagg(distinct pg.zkratka || ' (' || cts.zkratka || cfs.zkratka || ' '
                        || ro.cislo_rocniku || '.r) - ' || pg.nazev ,'; ')
                    within group(order by ro.cislo_rocniku) as popis
--            , pg.zkratka as pg, pg.nazev as program
--            , ob.zkratka as ob, ob.nazev as obor -- Pouze programy V1
--            , decode(ob.zkratka, '---',null, ob.zkratka) as ob -- Mix programu V1 a V2
--            , ob.zamereni as zam, decode(ob.zamereni, '--',null, ob.zamereni_nazev) as zamereni
--            , ob.ustav_id
--            , pg.rok_platnosti as rok
        from pr_tmp pr05                                                                -- *** Pr_tmp
             inner join st01.aktualni_predmet ap05                                      -- *** Aktualni predmet
                   on ( ap05.predmet_id = pr05.predmet_id
                        and pr05.rn = 1 -- Odstraneni duplicit vice aktualnich predmetu s ruznymi nazvy
                        and ap05.rok = pr05.rok
--                        and ap.rok=2011                 -- rok !!!
--                        and ap.typ_semestru_id = 1      -- 2-ZS, 1-LS
                        and ap05.status = 9
                       )
            inner join st01.predmet_planu pp05                                          -- *** Predmet planu
                  on ( pp05.aktualni_predmet_id = ap05.aktualni_predmet_id
                       and pp05.status = 9
                     )
            inner join st01.semestr se                                                  -- *** Semestr
                  on ( se.semestr_id = pp05.semestr_id
                       and se.status = 9
                     )
            inner join st01.rocnik ro                                                   -- *** Rocnik
                  on ( ro.rocnik_id = se.rocnik_id
                       and ro.status = 9 )
            inner join st01.stupen st                                                   -- *** Stupen
                  on ( st.stupen_id = ro.stupen_id
                       and st.status = 9 )
            inner join st01.obor ob                                                     -- *** Obor
                  on ( ob.obor_id = ro.obor_id
                       and ob.status = 9 )
            inner join st01.program pg                                                  -- *** Program
                  on ( pg.program_id = ob.program_id
                       and pg.status = 9 )
            inner join st01.c_typ_studia cts                                            -- *** C_typ_studia
                  on ( cts.typ_studia_id = pg.typ_studia_id
                       and cts.status = 9 )
            inner join st01.c_forma_studia cfs                                          -- *** C_forma_studia
                on ( cfs.forma_studia_id = pg.forma_studia_id
                     and cfs.status = 9 )
            inner join brutisadm.orgunit org                                            -- *** Orgunit
                  on org.orgunitid = pg.fakulta_id
        group by ap05.predmet_id
    ) -- as pp_tmp     ---------------------------

--select tmp.* from pp_tmp tmp
--/*
    --------------------------- Literatura akt. predmetu
    , prl_tmp as (select
            prl.fak, prl.rok, orgu.org_acronym as ust, ot.orgunit_name as ustav
            -- , prl.aktualni_predmet_id
            , ppt.ID_aktualnich_predmetu
            , prl.predmet_id
            , prl.pr, prl.predmet
            , li.literatura_id
            , ctl.popis as typ_literatury, li.poradove_cislo, li.jazyk
            , REPLACE(li.citace, chr(10), ' ') as citace
            , li.pub_id
            , ppt.fakulty_programu, ppt.typy, ppt.formy, ppt.programy, ppt.programy_nazvy
            , ppt.rocniky, ppt.popis
--            , row_number() over ( partition by li.literatura_id
--                                  order by prl.pr asc) as rn
        from pr_tmp prl                                                                 -- *** Pr_tmp
            inner join st01.literatura li                                               -- *** Literatura
                  on ( li.predmet_id = prl.predmet_id
                       and prl.rn = 1 -- Odstraneni duplicit vice aktualnich predmetu s ruznymi nazvy
                       and prl.rok between li.rok_od and nvl(li.rok_do, prl.rok)
                       and li.status = 9 )
            inner join st01.c_typ_literatury ctl                                        -- *** C_typ_literatury
                  on ctl.typ_literatury_id = li.typ_literatury_id

            left join pp_tmp ppt                                                        -- *** Pp_tmp
                 on ppt.predmet_id = prl.predmet_id
            left join brutisadm.orgunit orgu                                            -- *** Orgunit
                  on orgu.orgunitid = prl.ustav_id
            left join brutisadm.orgunit_translation ot
                  on ( ot.orgunitid = orgu.orgunitid
                       and ot.orgunit_trans_type = 'o'   -- Pro jednoduchost originalni nazev
                       and ot.status = 9
                       and ot.actual = 1
                      )
    ) -- as prl_tmp     --------------------------- Literatura akt. predmetu


select tmp.* from prl_tmp tmp
--where -- tmp.rn = 2
--    tmp.id_aktualnich_predmetu like '%,%'
--    tmp.literatura_id in (320910, 320911, 320912)
--order by tmp.literatura_id
order by fak, rok, ust, pr, typ_literatury desc, poradove_cislo, jazyk
/*

select count(distinct tmp.literatura_id) as pocet_literatura_id
from prl_tmp tmp
/*

-- */