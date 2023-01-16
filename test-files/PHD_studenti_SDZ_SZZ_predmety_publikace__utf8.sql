/* Za posledních 10 let 

  doktorand, datum nástupu, ročník, školitel, studijní program, ústav
--    Jak budou předměty řazeny je asi jedno, ale ano může to být podle data
--    zkoušky. Všechny předměty, které jsou součástí individuálního studijního
--    plánu schváleného oborovou radou ( podle SZR mohou být zakončeny, jak
--    doktorskou zkouškou tak i kolokviem).  Ostatní jako tělocvik jsou
--    nepodstatné.
  -- , termín zkoušky př1, termín zkoušky př2, termín zkoušky př3, termín zkoušky př4, termín zkoušky př5
--   Změnit termíny zk. na - počet zk. abs. v X ročníku (co sloupec, to č. ročníku obs. počet) 
  , termín SDZ, termín obhajoby DP
  -- Výjezdy asi nebudou evidovány, jde o stáže s délkou alespoň jeden měsíc.
  , výjezd
--    Jimp hlavní autor, Jimp, Jsc, Dsc, ost jsou typy VaV výsledku (v Apollu by
--    mělo být rozlišeno) a jak jsem psal stačí počty. 
  , Jimp hlavní autor, Jimp, Jsc
  , Dsc -- článek ve sborníku Wos nebo Scopus
  , ost -- ostatní typy publikací

  Doplnit:
  * forma studia,
  * pocet dokt. studii
  * publikace Jimp nebo JSC za poslednich 365 dnu
  
   
*/

with
    --------------------------- Rozsah roku, se kterymi se pracuje
    roky_tmp as 
      ( select 2022 - (Rownum-1) as rok     -- Nejvyssi rok
        from dual
        -- Connect by Rownum <= (2015-2015 +1) -- Nejvyssi rok - nejnizsi rok
        Connect by Rownum <= 10 -- Pocet let zpetne
      ) -- as roky_tmp     --------------------------- Rozsah roku, se kterymi se pracuje

-- select * from roky_tmp
-- /*
    --------------------------- Pocatky a konce akademickeho roku, se kterymi se pracuje
    , data_tmp as 
      ( select rt01.rok
               , to_date(rt01.rok || '-09-01', 'YYYY-MM-DD') as datum_od
               , to_date(rt01.rok+1 || '-08-31', 'YYYY-MM-DD') as datum_do
        from roky_tmp rt01
      ) -- as data_tmp      --------------------------- Pocatky a konce roku, se kterymi se pracuje
      
--select * from data_tmp
--/*

    --------------------------- Fakulta
    , fak_tmp as 
       ( select orgunit.orgunitid as fakulta_id, orgunit.org_acronym as fakulta
         from brutisadm.orgunit 
         where orgunit.orgunitid = 4 -- in ( 3, 5, 12, 13, 20042 ) -- 3 - FAST, 5 - FEKT, 12 - FCH, 13 - FP, 2042 - FIT  
       ) -- as fak_tmp --------------------------- Fakulta
       

--select * from fak_tmp
--/*


------------------------------------------------------------------------- Hlavni vypocet
-------------------------------------------------------------------------   

    --------------------------- ROPG  - Vybrane Rocniky - Programy
    , ropg_tmp as ( select pg.program_id, o.obor_id, st.stupen_id, r.rocnik_id   -- *** RoPg sekce
             , cts.zkratka as typ, cfs.zkratka as forma, pg.delka_studia 
             , st.cislo_stupne as stupen, r.cislo_rocniku as rocnik
             , pg.zkratka as pg, pg.nazev as program, o.zkratka as ob, o.nazev as obor 
             , o.zamereni as zam, decode(o.zamereni, '--',null, o.zamereni_nazev) as zamereni  
             , o.ustav_id, pg.fakulta_id, pg.rok_platnosti as rok
             , pg.typ_studia_id
        from st01.program pg
             inner join roky_tmp roky
                   on roky.rok = pg.rok_platnosti
             inner join fak_tmp fak                     -- Vybrane fakulty
                   on fak.fakulta_id = pg.fakulta_id
             inner join st01.obor o
                   on ( o.program_id = pg.program_id
                        and o.status = 9 and pg.status = 9 )
             inner join st01.stupen st
                    on ( o.obor_id = st.obor_id
                         and o.status = 9 )
             inner join st01.rocnik r
                  on ( st.stupen_id = r.stupen_id
                       and r.status = 9 )
             inner join st01.c_typ_studia cts
                  on ( cts.typ_studia_id = pg.typ_studia_id
                       and cts.zkratka = 'D'              -- Pouze doktorandi
                       and cts.status = 9 )
             inner join st01.c_forma_studia cfs
                on ( cfs.forma_studia_id = pg.forma_studia_id
                     and cfs.zkratka not in ('H','Z')                         -- Bez hostujicich programu (CZV)
                     and cfs.status = 9 )
    ) -- as ropg_tmp     --------------------------- ROPG  - Vybrane Rocniky - Programy
    
--select tmp.* from ropg_tmp tmp
--/*



    
    --------------------------- Studenti zapsaní do programu k datu
    , szs_tmp as
      ( select --+ MATERIALIZE
               fak.fakulta_id, fak.fakulta, data.rok
               , s.studium_id, pe.*, s.zacatek_studia
               , zs.stav_studia_id, cst.popis as stav_studia
               , s.aktivni_studium, s.poradi_studia
               , ropg.typ, ropg.forma as fo, ropg.delka_studia as del, ropg.stupen as st, ropg.rocnik as ro, ropg.pg
               , ropg.program, ropg.ob, ropg.obor, ropg.zam --, ropg.program_en, ropg.obor_en
               , ropg.typ_studia_id

               -- Pouze pro ladeni !!!!
--               , ropg.program_id
--               , to_date(data.rok || '-10-31', 'YYYY-MM-DD') as rozhodne_datum
               
        from
            st01.studium s                                                              -- *** Studium
            inner join fak_tmp fak                                                      -- *** Fakulta
                  on ( s.fakulta_id = fak.fakulta_id
                       -- and s.studium_id = 125962        -- Pouze pro ladeni!!!
                       and s.status = 9 ) 
            inner join st01.zmena_studia zs                                             -- *** Zmena studia
                  on ( zs.zmena_studia_id = s.posledni_zmena_studia_id          -- Vazba ven
--                        and to_date(data.rok || '-10-31', 'YYYY-MM-DD') 
--                                between zs.datum -- Kdyz neni zmena studia ukoncena, plati dokonce sk. roku
--                                        and nvl( zs.datum_do, to_date(zs.skolni_rok + 1 || '-08-31', 'YYYY-MM-DD'))
--                        and zs.stav_studia_id >= 60 -- Studium ukonceno v danem roce             
                        and zs.status = 9
                      )    
            inner join data_tmp data                                                    -- *** Data
                  on data.rok = zs.skolni_rok          -- Vazba ven - studenti zapsani do daneho roku
            inner join ropg_tmp ropg                                                    -- *** Ropg_tmp
                 on ropg.rocnik_id = zs.rocnik_id

            inner join st01.c_stav_studia cst                                           -- *** C_stav_studia
                  on ( cst.stav_studia_id = zs.stav_studia_id 
                       -- 2-zapis, 2-zarazeni, 4-financovani, 5-preruseni, 6-navrat
                       -- 9-preruseni aktivni
                       -- and cst.stav_studia_kategorie_id = 7 -- not in (1, 7, 8) -- 1-prevod, 7-ukonceni, 8-predzapis 
                     )                    
            inner join (select pe50.per_id as student_id
                       , pe50.label_pr as student 
                                  from brutisadm.person pe50                            -- *** Person
                                  where pe50.status = 9 ) pe
                  on pe.student_id = s.student_id

      ) -- as szs_tmp      --------------------------- Studenti zapsani do programu k datu
      
--select * from szs_tmp tmp
---- where tmp.program_id in (5552, 5547, 5690)
----where tmp.program_en = 'Engineering (B)'
----where tmp.zahr_cesta > 0
----      and tmp.typ = 'B'
---- where tmp.fakulta_id = 4
--/* 


    --------------------------- Dosavadni pocet dokt. studii na stejne fakulte
    , s_prev_tmp as (select sa.studium_id
            , count(s.studium_id) as poc_dosav_dokt_studii
        from szs_tmp sa                                                                 -- *** Szs_tmp
            inner join st01.studium s                                                   -- *** Studium
                  on ( s.student_id = sa.student_id
                       and s.poradi_studia >= sa.poradi_studia -- Toto studium a predchozi
                       and s.fakulta_id = sa.fakulta_id -- Stejna fakulta
                       and s.status = 9                 
                      )    
            inner join st01.zmena_studia zs                                             -- *** Zmena studia
                  on ( zs.zmena_studia_id = s.posledni_zmena_studia_id
                       -- zs.studium_id = s.studium_id -- Vazba pro aktualni_pro_rok
                       -- and zs.skolni_rok = 2019
                       -- and zs.aktualni_pro_rok = 1
                       -- and zs.stav_studia_id < 20                    -- zapsani studenti 
                       -- and zs.stav_studia_id > 1 
                       and zs.status = 9                 
                      )
            inner join st01.rocnik ro                                                   -- *** Rocnik 
                  on ( ro.rocnik_id = zs.rocnik_id
                       and ro.status = 9 )
            inner join st01.stupen st                                                   -- *** Stupen
                  on ( st.stupen_id = ro.stupen_id
                       and st.status = 9 )
            inner join st01.obor ob                                                     -- *** Obor 
                  on ( ob.obor_id = ro.obor_id
                       and ob.status = 9 )
            inner join st01.program pg                                                  -- *** Program
                  on ( pg.program_id = ob.program_id
                       and pg.typ_studia_id = sa.typ_studia_id      -- Stejneho typu
                       and pg.status = 9 )
        group by sa.studium_id
    ) -- as s_prev_tmp     --------------------------- Dosavadni pocet dokt. studii na FSI
    
--select tmp.* from s_prev_tmp tmp
--order by poc_dosav_dokt_studii desc
--/*


    --------------------------- Publikace
    -- Jimp hlavni, Jimp, Jsc, Dsc, ost
    , publikace_tmp as (select s15.studium_id, s15.student_id, s15.student
            , rp15.pub_id
            , pt15.pub_type, pt15.pub_type_full, pt15.m17_type
            , pp15.pers_pub_role, pp15.pers_pub_seq, pp15.pers_pub_portion
            , (case
                  when pt15.m17_type = 'Jimp' and pp15.pers_pub_seq = 1
                    then 1
                  else null
               end) as jimp_hlavni
            , (case
                  when pt15.m17_type = 'Jimp'
                    then 1
                  else null
               end) as jimp
            , (case
                  when pt15.m17_type = 'Jsc'
                    then 1
                  else null
               end) as jsc
            , (case
                  when pt15.m17_type = 'D'
                    then 1
                  else null
               end) as D
            , (case -- Ostatni typy publikaci, tj. to, co neni vyse
                  when pt15.m17_type not in ( 'Jimp', 'Jsc', 'D' )
                    then 1
                  else null
               end) as Publikace_ostatni
            , (case
                  when pt15.m17_type = 'Jimp'
                       and rp15.pub_date >= (sysdate-365) -- Publikace za posledni rok
                    then 1
                  else null
               end) as jimp_365
            , (case
                  when pt15.m17_type = 'Jsc'
                       and rp15.pub_date >= (sysdate-365) -- Publikace za posledni rok
                    then 1
                  else null
               end) as jsc_365
        from szs_tmp s15                                                                -- *** Szs_tmp 
            inner join brutisadm.pers_pub pp15                                          -- *** Pers_pub
                  on ( pp15.per_id = s15.student_id
                       and pp15.pers_pub_role != 'SU'        -- Spravce udaju !!!!
                       -- and pers_pub.pers_pub_role = 'G'        -- Pouze garant !!!!
                       and pp15.status = 9 )
            inner join brutisadm.pub_pers_role pr15                                     -- *** Pub_pers_role
                  on ( pr15.pub_pers_role = pp15.pers_pub_role
                     )
            inner join brutisadm.result_publication rp15                                -- *** Result_publication
                  on ( rp15.pub_id = pp15.pub_id
                       and rp15.status = 9 )
            inner join brutisadm.publication_type pt15                                  -- *** Publication_type
                  on ( pt15.pub_type = rp15.pub_type
                     )           

    ) -- as publikace_tmp     --------------------------- Publikace
    
--select tmp.* from publikace_tmp tmp
----where m17_type = 'Jsc'
--/*
--select distinct m17_type
--from publikace_tmp tmp
--/*

    --------------------------- Statistika publikaci
    , publikace_stat_tmp as (select p15.studium_id
            , sum(p15.jimp_hlavni) as jimp_hlavni
            , sum(p15.jimp) as jimp
            , sum(p15.jsc) as jsc
            , sum(p15.d) as d
            , sum(p15.publikace_ostatni) as publikace_ostatni
            , sum(p15.jimp_365) as jimp_365
            , sum(p15.jsc_365) as jsc_365
        from publikace_tmp p15                                                          -- *** Publikace_tmp
        group by p15.studium_id
    ) -- as publikace_stat_tmp     --------------------------- Statistika publikaci
    
--select tmp.* from publikace_stat_tmp tmp
--/*




    --------------------------- Skolitel - nejnovejsi
    , szs_sk as (select s10.studium_id
            , pe10.per_id as skolitel_id, pe10.skolitel
            , row_number() over ( partition by sk10.studium_id 
                                  order by sk10.od desc, nvl(sk10.do, sysdate) desc, sk10.skolitel_id) as rn 
        from szs_tmp s10                                                                -- *** Szs_tmp
            inner join st01.skolitel sk10                                               -- *** Skolitel 
                  on ( sk10.studium_id = s10.studium_id
                       and sk10.specialista = 0
                       and sk10.status = 9
                      ) 
            inner join (select pe50.per_id --, pe50.per_family_names as prijmeni, pe50.per_first_names as jmeno 
                        , pe50.label_pr as skolitel
                                   from brutisadm.person pe50                           -- *** Person
                                   where pe50.status = 9 ) pe10
                   on pe10.per_id = sk10.garant_id 
    ) -- as szs_sk     --------------------------- Skolitel - nejnovejsi
    
--select tmp.* from szs_sk tmp
--/*


    --------------------------- Dr_studium_ustav - nejnovejsi
    , szs_ust as (select s11.studium_id
            , org11.org_acronym as ustav
            , row_number() over ( partition by u11.studium_id 
                                  order by u11.datum_od desc, nvl(u11.datum_do, sysdate) desc
                                           , u11.dr_studium_ustav_id) as rn 
        from szs_tmp s11                                                                -- *** Szs_tmp
            inner join st01.dr_studium_ustav u11                                        -- *** Dr_studium_ustav
                  on ( u11.studium_id = s11.studium_id
                       and u11.status = 9
                      ) 
            left join brutisadm.orgunit org11                                           -- *** Orgunit 
                  on org11.orgunitid = u11.ustav_id
    ) -- as szs_ust     --------------------------- Dr_studium_ustav - nejnovejsi
    
--select tmp.* from szs_ust tmp
--/*



    --------------------------- Informace o statni zaverecne zkousce - pojednani + obhajoba
    , szav_tmp as (select s12.*
            , szc12.datum as datum_sdz
            , szc13.datum as datum_szz
            , row_number() over ( partition by s12.studium_id 
                                  order by szc12.datum desc, szc13.datum desc) as rn 
        from szs_tmp s12                                                                -- *** Szs_tmp
            left join st01.szz szz12                                                    -- *** Szz
                 on szz12.studium_id = s12.studium_id
            left join st01.szz_cast szc12                                               -- *** Szz_cast - pojednani
                  on ( szc12.szz_id = szz12.szz_id
                       and szc12.szz_cast_typ_id = 1  -- U doktorandu 1-pojednani, 2-obhajoba dizertace
                       and szc12.datum is not null    -- Tj. je stanoveno datum SDZ - musi byt prihlasen
                       and szc12.status = 9 )
            left join st01.c_szz_cast_typ szt12                                         -- *** C_szz_cast_typ 
                  on ( szt12.szz_cast_typ_id = szc12.szz_cast_typ_id
                       and szt12.status = 9 )
            left join st01.szz_cast szc13                                               -- *** Szz_cast - obhajoba
                  on ( szc13.szz_id = szz12.szz_id
                       and szc13.szz_cast_typ_id = 2  -- U doktorandu 1-pojednani, 2-obhajoba dizertace
                       and szc13.datum is not null    -- Tj. je stanoveno datum SDZ - musi byt prihlasen
                       and szc13.status = 9 )
            left join st01.c_szz_cast_typ szt13                                         -- *** C_szz_cast_typ 
                  on ( szt13.szz_cast_typ_id = szc13.szz_cast_typ_id
                       and szt13.status = 9 )
    ) -- as szav_tmp     --------------------------- Informace o statni zaverecne zkousce - pojednani + obhajoba
    
--select tmp.* from szav_tmp tmp
----where rn > 1
----where studium_id in (92369, 133555)
--/*


    --------------------------- Absolvovane drzk a kol
    , szs_ei_tmp as (select s13.studium_id
            , ei13.el_index_id
            , nvl(ei13.datum_zapisu_vysledku, ei13.datum_zapoctu) as datum
            , cu13.zkratka
            , 'r_' || nvl(to_char(ro13.cislo_rocniku), 'uzn') as rocnik_abs
--            , 'p' || row_number() over ( partition by s13.studium_id 
--                                  order by nvl(ei13.datum_zapisu_vysledku, ei13.datum_zapoctu)) as predmet_c 
        from szs_tmp s13                                                                -- *** Szs_tmp
            inner join st01.el_index ei13                                               -- *** El_index 
                  on ( ei13.studium_id = s13.studium_id
                       and ei13.absolvoval = 1  -- Pouze absolvovane
                       and ei13.status = 9 )
            inner join st01.c_typ_ukonceni_predmetu cu13                                -- *** C_typ_ukonceni_predmetu
                  on ( cu13.typ_ukonceni_predmetu_id = ei13.typ_ukonceni_predmetu_id
                       and cu13.typ_ukonceni_predmetu_id in (20, 21, 5, 25) 
                       -- and .status = 9 
                     )
            left join st01.zmena_studia zs13                                            -- *** Zmena_studia - ve kt. rocniku abs?
                  on ( zs13.studium_id = ei13.studium_id
                       and nvl(ei13.datum_zapisu_vysledku, ei13.datum_zapoctu) 
                               between zs13.datum and nvl(zs13.datum_do -1, sysdate) -- Datum abs. spada do platnosti zmeny studia 
                               -- Datum_do = datum nasledujici zmeny studia. Proto tam musi byt -1, aby nebyly prekryvy
                       and zs13.status = 9 ) 
            left join st01.rocnik ro13                                                  -- *** Rocnik 
                  on ( ro13.rocnik_id = zs13.rocnik_id
                       and ro13.status = 9 )
    ) -- as szs_ei_tmp     --------------------------- Absolvovane drzk a kol
    
--select tmp.* from szs_ei_tmp tmp
----where tmp.rocnik_abs = 'r_'
--where tmp.studium_id = 152623
----where predmet_c = 'p9'
--/*

    --------------------------- Celkovy pocet zkousek
    , szs_ei_celk_tmp as (select se13.studium_id
            , count(distinct se13.el_index_id) as predmetu_celkem
        from szs_ei_tmp se13                                                            -- *** Szs_ei_tmp
        group by se13.studium_id
    ) -- as _tmp     --------------------------- Celkovy pocet zkousek
    
--select tmp.* from szs_ei_celk_tmp tmp
--/*

    --------------------------- Priprava pro pivot
    , priprava_tmp as (
--        select listagg( '''' || vf.predmet_c || ''' as "' || upper( vf.predmet_c ) || '"' , ', ' ) 
--             within group (order by vf.predmet_c)
--              as pozn
--         from (select distinct tmp.predmet_c
--               from szs_ei_tmp tmp 
--              ) vf
        select listagg( '''' || vf.rocnik_abs || ''' as "' || upper( vf.rocnik_abs ) || '"' , ', ' ) 
             within group (order by vf.rocnik_abs)
              as pozn
         from (select distinct tmp.rocnik_abs
               from szs_ei_tmp tmp 
              ) vf
    ) -- as priprava_tmp     --------------------------- Priprava pro pivot
    
--select tmp.* from priprava_tmp tmp
--/*
---- 'r_1' as "R_1", 'r_2' as "R_2", 'r_3' as "R_3", 'r_4' as "R_4", 'r_5' as "R_5", 'r_6' as "R_6", 'r_7' as "R_7", 'r_uzn' as "R_UZN"
---- 'p1' as "P1", 'p2' as "P2", 'p3' as "P3", 'p4' as "P4", 'p5' as "P5", 'p6' as "P6", 'p7' as "P7", 'p8' as "P8", 'p9' as "P9", 'p10' as "P10", 'p11' as "P11"

    --------------------------- Pivot predmetu el. indexu 
    , ei_pivot_tmp as (
        select *
        from ( select studium_id, el_index_id, rocnik_abs
               from szs_ei_tmp )

        pivot ( -- max (datum) as datum
                count (el_index_id)
                -- Teoreticky se sem muze dat i dalsi sloupec
                -- , sum (pocet_neceho) as suma
                for rocnik_abs 
                in ('r_1' as "R_1", 'r_2' as "R_2", 'r_3' as "R_3", 'r_4' as "R_4"
                    , 'r_5' as "R_5", 'r_6' as "R_6", 'r_7' as "R_7", 'r_uzn' as "R_UZN"
                  ) -- Sem se musi dat konstanty, subselect jde jen s pivot XML
               )
    ) -- as ei_pivot_tmp     --------------------------- Pivot predmetu el. indexu 
    
--select tmp.* from ei_pivot_tmp tmp
--/*
--select tmp.p1_DATUM
--from ei_pivot_tmp tmp
--/*


    --------------------------- Zahranicni cesty studentu (i po CR), alespon 30 dnu
    , zahr_cesta_tmp as (select  szs.fakulta_id, szs.fakulta 
                , szs.studium_id, szs.student_id, szs.student
                , szs.typ, szs.fo, szs.del, szs.pg, szs.ob, szs.zam
                , czc.popis as typ_cesty
                , zc.zahranicni_cesta_id, zc.celkem_od, zc.celkem_do
                , round(zc.celkem_do - zc.celkem_od,0) as delka_pob
--                , round(least(zc.celkem_do, dt.datum_do) 
--                        - greatest(zc.celkem_od, dt.datum_od),0) as delka_pob_obdobi -- Delka pobytu spadajici do daneho obdobi
                , szs.rok
                , zc.firma, zc.rp_univerzita
                , zc.poznamka, zc.univerzita_id, ot.orgunit_name
                , zc.country_code as zc_country_code
                , ( select listagg( co.country_code, ', ' ) 
                        within group (order by co.country_code)
                         as pozn
                    from ( select distinct co2.orgunitid, co2.country_code
                           from brutisadm.contact co2                                   -- *** Contact
                           where co2.orgunitid = org.orgunitid
                                -- and co.org_con_role = 'SPEC' -- Typ kontaktu je nevhody pro filtrovani mnohonasobnych kontaktu
                                and co2.per_id is null  -- Takto se odfiltruji vztahy mezi ext. orgunitami a osobami 
                                and co2.status = 9
                         ) co
                      ) as country_codes
                , ( select listagg( oty.org_type, ', ' ) 
                        within group (order by oty.org_type)
                         as pozn
                    from brutisadm.orgunit_type oty                   -- *** Orgunit_type
                         inner join brutisadm.org_type_org oto        -- *** Org_type_org
                             on ( oto.org_type = oty.org_type
                                  -- and oty.org_type not in ('FA', 'HES', 'SO') -- FA-fakulta, HES-vysoke skoly, SO-soucast VS
                                )
                    where oto.orgunitid = org.orgunitid -- Vazba ven
                           ) as org_types    -- Typy organizace navazane na zahr. cestu
                ---- Nasleduje priorita zobrazeni cesty v tom ak. roce, ve kterem je vetsi cast cesty
--                , row_number() over ( partition by zc.zahranicni_cesta_id
--                                        order by (least(zc.celkem_do, dt.datum_do) - greatest(zc.celkem_od, dt.datum_od)) desc
--                                                 , szs.rok, zc.country_code) rn 
        from szs_tmp szs                                                                -- *** Studenti a zmeny studia
--             inner join data_tmp dt                                                     -- *** Rozsah dat (kvuli datum od-do)
--                   on dt.rok = szs.rok
             inner join st01.zahranicni_cesta zc                                        -- *** Zahranicni cesta
                  on ( zc.studium_id = szs.studium_id
                       and zc.status = 9
--                       and zc.celkem_od < dt.datum_do
--                       and zc.celkem_do > dt.datum_od
                        and round(zc.celkem_do - zc.celkem_od,0) >= 30 -- Minimalni delka 1 mesic
--                       and zc.typ_zahranicni_cesty_id  -- Teoreticky by se dalo ridit i sloupcem TYP_STAZ, ale ten neni presny (a nepoznaji se ofic. cesty) 
--                            in (4, 18, 19, 22, 25) -- 4-Neuvedeno, pracovni pobyty Erasmus, Erasmus+, AIAESTE
--                       and lower(nvl(zc.poznamka,'nic')) not like '%universi%'     -- U nekterych pobytu neni vyplnena orgunita, je v poznamce
--                       and lower(nvl(zc.poznamka,'nic')) not like '%hochschule%'
--                       and lower(nvl(zc.firma,'nic')) not like '%universi%'        -- U nekterych pobytu neni vyplnena orgunita, je vyplnena textem zde
--                       and lower(nvl(zc.firma,'nic')) not like '%hochschule%'
--                       and lower(nvl(zc.firma,'nic')) not like '%school of physical sciences%' -- To je soucast Univerzity of Kent 
                     )
            inner join st01.c_typ_zahranicni_cesty czc                                  -- *** c_typ_zahranicni_cesty
                  on ( czc.typ_zahranicni_cesty_id = zc.typ_zahranicni_cesty_id
                       and czc.status = 9 )
            left join brutisadm.orgunit org                                             -- *** Orgunit 
                  on org.orgunitid = zc.univerzita_id
            left join brutisadm.orgunit_translation ot
                  on ( ot.orgunitid = org.orgunitid
                       and ot.orgunit_trans_type = 'o'   -- Pro jednoduchost originalni nazev
                       and ot.status = 9
                       and ot.actual = 1
                      )

    ) -- as zahr_cesta_tmp     --------------------------- Zahranicni cesty studentu (i po CR), alespon 30 dnu
    
--select tmp.* from zahr_cesta_tmp tmp
----where 
------    and tmp.rok >= 2013 
----    and tmp.fakulta_id = 4
------      and tmp.typ_cesty like 'Jiný%'
------      and lower(tmp.poznamka) not like '%university%'
--/*

    --------------------------- Zahranicni cesty - pocet na studium_id
    , zahr_cesta_stat_tmp as (select zct.studium_id
            , count (distinct zct.zahranicni_cesta_id) as pocet_zc
        from zahr_cesta_tmp zct                                                         -- *** Zahr_cesta_tmp
        group by zct.studium_id
    ) -- as zahr_cesta_stat_tmp     --------------------------- Zahranicni cesty - pocet na studium_id
    
--select tmp.* from zahr_cesta_stat_tmp tmp
--/*



---------------------- Main selecty


---------------------------  Vsichni studenti seznam
--    * seznam radne ukoncenych:
--      ** delka studia, datum SDZ a datum obhajoby
select s.student_id, s.student, s.aktivni_studium
    , s.fo as forma, sp.poc_dosav_dokt_studii
    , s.rok, s.zacatek_studia, s.ro as rocnik
    , sk.skolitel_id, sk.skolitel
    , s.program
    , ust.ustav
    -- Rocniky a pocet abs. predmetu drzk a kol
    , ei.r_1, ei.r_2, ei.r_3, ei.r_4, ei.r_5, ei.r_6, ei.r_7, ei.r_uzn
    , sec.predmetu_celkem
--    -- Terminy absolvoani drzk a kol
--    , ei.p1_datum, ei.p2_datum, ei.p3_datum, ei.p4_datum, ei.p5_datum
--    , ei.p6_datum, ei.p7_datum, ei.p8_datum, ei.p9_datum, ei.p10_datum
--    , ei.p11_datum
    -- Statni zav. zkouska, statni doktorska zkouska
    , z.datum_sdz
    , z.datum_szz   
    -- Zahranicni cesty (i po CR)
    , zc.pocet_zc as vyjezd
    -- Publikace
    , ps.jimp_hlavni, ps.jimp, ps.jsc, ps.d, ps.publikace_ostatni
    , ps.jimp_365, ps.jsc_365
from szs_tmp s                                                                          -- *** Szs_tmp
    left join szs_sk sk                                                                 -- *** Szs_sk
         on ( sk.studium_id = s.studium_id
              and sk.rn = 1 -- Pouze posledni (nejnovejsi) skolitel
            )
    left join szs_ust ust                                                               -- *** Szs_ust
         on ( ust.studium_id = s.studium_id
              and ust.rn = 1 -- Pouze posledni (nejnovejsi) skolitel
            )
    left join ei_pivot_tmp ei                                                           -- *** Ei_pivot_tmp
         on ( ei.studium_id = s.studium_id
            )
    left join szs_ei_celk_tmp sec                                                       -- *** Szs_ei_celk_tmp
         on ( sec.studium_id = s.studium_id
            )
    left join szav_tmp z                                                                -- *** Szav_tmp
         on ( z.studium_id = s.studium_id
              and z.rn = 1 -- Pouze posledni termin pojednani a obhajoby
            )
    left join zahr_cesta_stat_tmp zc                                                    -- *** Zahr_cesta_stat_tmp
         on ( zc.studium_id = s.studium_id
            )
    left join publikace_stat_tmp ps                                                     -- *** Publikace_stat_tmp
         on ( ps.studium_id = s.studium_id
            )
    left join s_prev_tmp sp                                                             -- *** S_prev_tmp
         on sp.studium_id = s.studium_id
         
--where s.aktivni_studium = 1 -- Pro vyplaceni stipendii pouze aktivni studenti

order by s.rok, s.program, ust.ustav, s.student
/*





-- */      