/* Studijni plany kontrola povinnych prerekvizit a korekvizit
   Kontrola, zda kdyz se zaradi prerekvizity do studijniho planu, zda nebudou mit
   nejaci studenti problem.
      Kontrolovat:
    * existujici prerekvizity
    * navrh prerekvizit (predmety zadane zkratkou) 
   Filtry
    * FILTR_PR_PG = 1 -- Filtrovat predmety fakulty/ustavu ve vsech programech (vsech fakult)
    * FILTR_PR_PG = 2 -- Filtrovat programy fakulty/ustavu se vsemi predmety (vsech fakult)

   PS: tenhle SQL je zamotany jak misa spaget. Omlouvam se. 
       Schema k tomuto SQL je v souboru Plany_prerekvizity_kontrola.dia
       https://sourceforge.net/projects/dia-installer/
   
   Plany_prerekvizity_kontrola.sql
   Verze
   * 2022-07-22 - pridana podpora volby jaz. verze, vyhodnoceni ekv. a uznatelnych predmetu
   * 2022-07-18 - oprava filtru, doplneni chybejici vazby (odstranuje zdvojene radky)
   * 2022-07-08 - pridan sloupec predmet_planu_id.
   * 2022-07-07 - prvni verze.
*/
with
    --------------------------- Paleta barev pro barveni radku
    -- ID1 je razeni barev blizko sebe, ID2 dava vedle sebe hodne odlisne barvy
    -- barva (barva_id, barva, barva_id2) as (
    barva as (
          select 0 as barva_id, 9815779 as barva, 0 as barva_id2 from dual union all
          select 1, 9606899, 4 from dual union all
          select 2, 14533356, 7 from dual union all
          select 3, 14729164, 1 from dual union all
          select 4, 15256241, 5 from dual union all
          select 5, 15066037, 8 from dual union all
          select 6, 13364446, 2 from dual union all
          select 7, 13760767, 6 from dual union all
          select 8, 12378109, 9 from dual union all
          select 9, 15725041, 3 from dual
    ) -- as barva     --------------------------- Paleta barev pro barveni radku

--select tmp.* from barva tmp
--/*

    --------------------------- Rocniky a programy VUT 4 ak. let
    , ropg as (select pg.program_id, ob.obor_id, st.stupen_id, r.rocnik_id
             , cts.zkratka as typ, cfs.zkratka as forma, pg.delka_studia 
             , st.cislo_stupne as stupen, r.cislo_rocniku as rocnik
             , pg.zkratka as pg, pg.nazev as program
             -- , ob.zkratka as ob, ob.nazev as obor -- Pouze programy V1
             , decode(nvl(ob.bez_specializace,0),0,ob.zkratka, null) as ob -- Mix programu V1 a V2
             , decode(nvl(ob.bez_specializace,0),0,ob.nazev, null) as obor -- Mix programu V1 a V2
             , ob.zamereni as zam, decode(ob.zamereni, '--',null, ob.zamereni_nazev) as zamereni 
             , pg.verze, ob.ustav_id, pg.fakulta_id
             , pg.rok_platnosti as rok, pg.lang_code as jaz, csp.zkratka as spol_pg
             , ob.obor_lonsky_id, pg.garant_id
        from st01.program pg                                                            -- *** Program
             inner join st01.obor ob                                                    -- *** Obor
                   on ( ob.program_id = pg.program_id
                        and pg.status = 9 
                        -- and pg.fakulta_id = 4 -- Zakomentovano kvuli mezifak. vyuce
                        ---- BIND !!!
                        and pg.rok_platnosti between (:AKROK - 3) and :AKROK -- -3 max. delka bak. studia na VUT = 4 roky
--                        and pg.rok_platnosti between 2018 and 2021
                        and ob.status = 9 )
             inner join st01.stupen st                                                  -- *** Stupen 
                    on ( st.obor_id = ob.obor_id 
                         and st.status = 9 )
             inner join st01.rocnik r                                                   -- *** Rocnik
                  on ( r.stupen_id = st.stupen_id
                       and r.status = 9 )
             inner join st01.c_typ_studia cts                                           -- *** C_typ_studia
                  on ( cts.typ_studia_id = pg.typ_studia_id
                       -- and cts.status = 9 
--                       and cts.zkratka in ('B', 'M', 'N')
                      )
             inner join st01.c_forma_studia cfs                                         -- *** C_forma_studia
                  on ( cfs.forma_studia_id = pg.forma_studia_id
                       -- and cfs.status = 9 
                      )    
             inner join st01.c_spolecny_program csp                                      -- *** C_spolecny_program
                  on ( csp.spolecny_program_id = pg.spolecny_program_id
                       -- and .status = 9 
                     )
    ) -- as RoPg --------------------------- Rocniky a programy VUT 4 ak. let
   
--select tmp.* from ropg tmp
--/*


    --------------------------- Rocniky zkoumaneho roku, fakulty a ustavu
    , ropg_act as (select *
        from ropg                                                                       -- *** Ropg
        where
--            ---- BIND !!!
            rok = :AKROK
--            rok = 2021
            ---- BIND !!!
            and (   ( :FILTR_PR_PG = 2 -- Filtrovat programy, fakulta kontroluje prerekvizity v predmetech sveho stud. planu
                      and fakulta_id = :FAKULTA_ID
                      and ( ustav_id = :USTAV_ID
                            or :USTAV_ID = -1 )
                    )
                 or :FILTR_PR_PG != 2
               )
--            and fakulta_id = 4
--            and ustav_id = 157
    ) -- as ropg_act     --------------------------- Rocniky zkoumaneho roku, fakulty a ustavu
    
--select tmp.* from ropg_act tmp
--/*

    --------------------------- Studijni plany aktualniho roku, bez jazykovych verzi
    , plany_act_raw as (select ro05.rocnik_id, pp05.aktualni_predmet_id
            , pp05.predmet_planu_id, pp05.povinnost, pp05.zmena_jazyka
            , ap05.predmet_id, pr05.zkratka as pr, pr05.fakulta_id as pr_fakulta_id
            , ro05.fakulta_id as pp_fakulta_id, ro05.ustav_id as pp_ustav_id
            , ap05.ustav_id as ap_ustav_id
            , ro05.rok, se05.typ_semestru_id
        from ropg_act ro05                                                              -- *** Ropg_act
            inner join st01.semestr se05                                                -- *** Semestr 
                  on ( se05.rocnik_id = ro05.rocnik_id
                       and se05.status = 9 )
            inner join st01.predmet_planu pp05                                          -- *** Predmet_planu 
                  on ( pp05.semestr_id = se05.semestr_id
                       -- and pp05.povinnost in (2, 13)      -- 2-P, 13-PV
                       and pp05.status = 9 )
            inner join st01.aktualni_predmet ap05                                       -- *** Aktualni predmet
                  on ( ap05.aktualni_predmet_id = pp05.aktualni_predmet_id
                       and ap05.status = 9
                     )
            inner join st01.predmet pr05                                                -- *** Predmet
                  on ( pr05.predmet_id = ap05.predmet_id  
                       and pr05.status = 9
                      )
    ) -- as plany_act_raw     --------------------------- Studijni plany aktualniho roku, bez jazykovych verzi

--select tmp.* from plany_act_raw tmp
----where predmet_planu_id = 616953
--order by rok desc, rocnik_id, povinnost, pr
--/*

    --------------------------- Studijni plany pouze jazykovych verzi, mozne zdvojeni kvuli akt. predm. 
    , plany_act_ekv_tmp as (select pn05.rocnik_id, apek05.aktualni_predmet_id
            , pn05.predmet_planu_id, pn05.povinnost, pn05.zmena_jazyka
            , prek05.predmet_id as predmet_id, prek05.zkratka as pr
            , prek05.fakulta_id as pr_fakulta_id
            , pn05.pp_fakulta_id, pn05.pp_ustav_id
            , apek05.ustav_id as ap_ustav_id
            , pn05.rok, pn05.typ_semestru_id
            , row_number() over ( partition by pn05.predmet_planu_id, prek05.predmet_id 
                                  order by decode(apek05.typ_semestru_id, pn05.typ_semestru_id,0, 1)
                                        , apek05.typ_semestru_id
                                ) as rn 
            -- , 1 as jazykova_erze
            -- Pro ladeni
--            , pr05.predmet_id, pr05.zkratka as pr, pr05.fakulta_id as pr_fakulta_id
--            , prek05.zkratka as pr_ek
--            , apek05.typ_semestru_id as apek05_typ_semestru_id
        from plany_act_raw pn05                                                         -- *** Plany_act_raw
            inner join st01.ekvivalentni_predmety eps05                                 -- *** Ekvivalentni_predmety - skupina
                  on ( eps05.predmet_id = pn05.predmet_id
                       and pn05.zmena_jazyka = 1 -- Pouze tam, kde je povolena zmena jazyka
                       and eps05.status = 9 ) 
            inner join st01.ekvivalentni_predmety ep05                                  -- *** Ekvivalentni predmety - dalsi predmet
                  on ( ep05.skupina_id = eps05.skupina_id 
                       and ep05.predmet_id != pn05.predmet_id -- Bez orig. predmetu
                       and ep05.status = 9
                     )
            inner join st01.predmet prek05                                              -- *** Predmet - ekvivalentni predmet
                  on ( prek05.predmet_id = ep05.predmet_id
                       and prek05.status = 9
                     )
            inner join st01.aktualni_predmet apek05                                     -- *** Aktualni_predmet - ekvivalentni predmet
                  on ( apek05.predmet_id = prek05.predmet_id
                       and apek05.rok = pn05.rok     -- Ekvivalentni akt. predmet musi byt ve stejnem roce, semestr se filtruje pomoci rn
                       and apek05.status = 9
                     )
    ) -- as plany_act_ekv_tmp     --------------------------- Studijni plany pouze jazykovych verzi, mozne zdvojeni kvuli akt. predm. 
    
--select tmp.* from plany_act_ekv_tmp tmp
--order by rocnik_id, predmet_planu_id, pr
--/*

            
    --------------------------- Studijni plany aktualniho roku vcetne jazykovych verzi
    , plany_act_tmp as (
        select pw05.*, 0 as jazykova_verze 
        from plany_act_raw pw05                                                         -- *** Plany_act_raw
        union all
        select pe05.*
            -- , 1 as jazykova_verze - Misto 1 (jazykova verze) je tu sloupec rn = 1 ;-)
        from plany_act_ekv_tmp pe05                                                     -- *** Plany_act_ekv_tmp
        where pe05.rn = 1 -- Pouze nejvhodnejsi akt. predmet daneho roku (nejlepe stejny semestr)
     
    ) -- as plany_act_tmp     --------------------------- Studijni plany aktualniho roku vcetne jazykovych verzi
    
--select tmp.* from plany_act_tmp tmp
----where predmet_planu_id = 616953
----where rocnik_id = 35434
--order by rok desc, rocnik_id, povinnost, pr
--/*


    --------------------------- Predmety studijniho planu zkoumaneho roku pripadne ustavu
    -- Tyto predmety mohou mit prerekvizitu nebo korekvizitu
    -- Bud se berou:
    -- * predmety fakulty ze vsech planu :FILTR_PR_PG = 1
    -- * predmety z planu fakulty :FILTR_PR_PG = 2 (zajistuje ropg_sel_act)
    , pr_planu_sel_act_tmp as (
        select -- distinct pt05.predmet_id, pt05.pr, pt05.pr_fakulta_id
            pt05.*
        from plany_act_tmp pt05                                                         -- *** Plany_act_tmp
            inner join ropg_act rs05                                                    -- *** Ropg_act - filtr na zkoumany rok, fakultu, ustav
                  on ( rs05.rocnik_id = pt05.rocnik_id
                       ---- BIND !!!
                       and pt05.rok = :AKROK
--                        and pt05.rok = 2021
                       ---- BIND !!!
                       and (   ( :FILTR_PR_PG = 1 -- Filtrovat predmety, fakulta kontroluje prerekvizity ve svych predmetech
                                  and pt05.pr_fakulta_id = :FAKULTA_ID
                                  and ( pt05.ap_ustav_id = :USTAV_ID
                                        or :USTAV_ID = -1
                                      ) 
                                )
                             or :FILTR_PR_PG != 1
                           )
                     )
    ) -- as pr_planu_sel_act_tmp     --------------------------- Predmety studijniho planu zkoumaneho roku roku pripadne ustavu
    
--select tmp.* from pr_planu_sel_act_tmp tmp
--/*

--    --------------------------- Zkoumane predmety s prerekvizitou (vcetne jejich jaz. ekvivalentu)
--    , pr_sel_tmp as (select distinct predmet_id, pr, pr_fakulta_id
--        from pr_planu_sel_act_tmp                                                  -- *** pr_planu_sel_act_tmp
--    ) -- as pr_sel_tmp     --------------------------- Zkoumane predmety s prerekvizitou (vcetne jejich jaz. ekvivalentu)
--    
--select tmp.* from pr_sel_tmp tmp
--/*
    --------------------------- Predmety planu majici prerekvizity + fake prerekvizity
    -- Fake prerekvizitami je mozne udelat si v SQL simulaci neexistujici prerekvizity.
    -- Jednodussi je ale vlozit prerekvizitu na testovaci databazi...
    , predm_prer_tmp as (
        ---- Existujici prerekvizity
        select --+ MATERIALIZE
            prp.*
            , re.prerekvizita_id, re.typ_prerekvizity as typ_prerekvizity_id, re.min_hodnoceni
            , pre.predmet_id as prer_predmet_id, pre.zkratka as prer_pr
            , pre.fakulta_id as prer_fakulta_id
            , 'existující prerekvizita' as info
        from
            ( select distinct predmet_id, pr, pr_fakulta_id
              from pr_planu_sel_act_tmp                                                 -- *** Pr_planu_sel_act_tmp
              where
                   ---- BIND !!!
                   (   ( :FILTR_PR_PG = 1 -- Filtrovat predmety, fakulta kontroluje prerekvizity ve svych predmetech
                         and ( ap_ustav_id = :USTAV_ID
                               or :USTAV_ID = -1 )
                       )
                    or :FILTR_PR_PG != 1
                  )
                ) prp
            inner join st01.prerekvizita re                                             -- *** Prerekvizita
                  on ( re.predmet_id_1 = prp.predmet_id
                       and re.typ_prerekvizity in (1, 2) -- 1-pov. prer., 2-pov. kor.
                       and re.status = 9 )
            inner join st01.predmet pre                                                 -- *** Predmet - je prerekvizitou
                  on ( pre.predmet_id = re.predmet_id_2 
                       -- and pre.zkratka like '9%'
                       and pre.status = 9
                      )
--        union all
--        ---- Navrh prerekvizity - zde se da vytvorit fiktivni prerekvizita a ta testovat
--        -- Ale asi je jednodussi prerekvizitu vlozit na testovaci databazi
--        select --+ MATERIALIZE
--            prp.*
--            , -1* rownum as prerekvizita_id
--            -- , re.typ_prerekvizity as typ_prerekvizity_id
--            , (case
--                  when prp.pr in ( 'neex' ) and pre.zkratka in ( 'neex' ) then 1
--                  else 1 -- Default, nejcastejsi prerekvizita
--               end) as typ_prerekvizity_id
--            , null as min_hodnoceni
--            , pre.predmet_id as prer_predmet_id, pre.zkratka as prer_pr, pre.fakulta_id as prer_fakulta_id
--            , 'návrh' as info
--        from
--            pr_planu_sel_act_tmp prp                                                    -- *** Pr_planu_sel_act_tmp - ma prerekvizitu
--            inner join st01.predmet pre                                                 -- *** Predmet - je prerekvizitou
--                  on ( prp.pr in ('6KT', '6KT-A') -- Predmety navic, ktere budou mit prerekvizitu
--                       and prp.pr_fakulta_id = 4 -- Predmety navic se musi filtrovat pres fakultu
--                       -- Kombinace zdrojovych predmetu a jejich prerekvizit
--                       and  (case
--                                when prp.pr in ( '6KT', '6KT-A' ) and pre.zkratka in ('7AZ') then 1
--                                else 0
--                             end) = 1
--                       -- and pre.zkratka like '9%'
--                       and pre.status = 9
--                      )
                      
    ) -- as predm_prer_tmp     --------------------------- Predmety planu majici prerekvizity + fake prerekvizity
    
--select tmp.* from predm_prer_tmp tmp 
--/*

    --------------------------- Distinktni seznam zkoumanych prerekvizit
    , prer_dist_tmp as (select distinct prer_predmet_id, prer_pr, prer_fakulta_id, info 
        from predm_prer_tmp
    ) -- as _tmp     --------------------------- Distinktni seznam zkoumanych prerekvizit
    
--select tmp.* from prer_dist_tmp tmp
--/*


    --------------------------- Prerekvizity a jejich ekvivalentni/uznatelne predmety
    , prer_ekv_tmp as (
        ---- Samotne prerekvizity
        select pd20.prer_predmet_id, pd20.prer_pr
            , pd20.prer_predmet_id as ekv_predmet_id, pd20.prer_pr as ekv_pr 
        from prer_dist_tmp pd20                                                         -- *** Prer_dist_tmp
        union
        ---- Jazykove ekvivalenty prerekvizit
        select p20.prer_predmet_id, p20.prer_pr
            , pr20.predmet_id as ekv_predmet_id, pr20.zkratka as ekv_pr
        from prer_dist_tmp p20                                                          -- *** Prer_dist_tmp
        inner join st01.ekvivalentni_predmety eps20                                     -- *** Ekvivalentni_predmety - skupina
              on ( eps20.predmet_id = p20.prer_predmet_id
                   and eps20.status = 9
                 )
        inner join st01.ekvivalentni_predmety ep20                                      -- *** Ekvivalentni_predmety - ostatni predmety
              on ep20.skupina_id = eps20.skupina_id 
                 and ep20.predmet_id != p20.prer_predmet_id -- Ostatni predmety bez predmetu samotnych
                 and ep20.status = 9
        inner join st01.predmet pr20
           on pr20.predmet_id = ep20.predmet_id
              and pr20.status = 9
        union
        ---- Uznatelne predmety prerekvizit
        select p20.prer_predmet_id, p20.prer_pr
            , pr20.predmet_id as ekv_predmet_id, pr20.zkratka as ekv_pr
        from prer_dist_tmp p20                                                          -- *** Prer_dist_tmp
        inner join st01.predmet_uznatelny pu20                                          -- *** Predmet_uznatelny
              on ( pu20.predmet_id = p20.prer_predmet_id
                   and pu20.status = 9
                 )
        inner join st01.predmet pr20
           on pr20.predmet_id = pu20.predmet_uznany_id 
              and pr20.status = 9
    ) -- as prer_ekv_tmp     --------------------------- Prerekvizity a jejich ekvivalentni/uznatelne predmety
    
--select tmp.* from prer_ekv_tmp tmp
--order by prer_pr, ekv_pr
--/*

    --------------------------- Prerekvizity a jejich nazvy (pro finalni vypis)
    -- Pro pripad, kdy by se prerekvizita v predchozich planech vubec nenasla
    , prer_ap_tmp as (select prer20.prer_predmet_id, ap20.aktualni_predmet_id as prer_ap_id
            , ap20.nazev as prer_predmet
            , ap20.ustav_id as prer_ustav_id
            , row_number() over ( partition by prer20.prer_predmet_id 
                                  order by ap20.rok desc, ap20.typ_semestru_id desc) as rn 
        from (select distinct prer_predmet_id
              from predm_prer_tmp                                                       -- *** Predm_prer_tmp
             ) prer20
            inner join st01.aktualni_predmet ap20                                       -- *** Aktualni predmet
                  on ( ap20.predmet_id = prer20.prer_predmet_id
                        ---- BIND !!!
                        and ap20.rok between (:AKROK - 3) and :AKROK -- -3 max. delka bak. studia na VUT = 4 roky
--                       and ap20.rok between 2018 and 2021
                       and ap20.status = 9
                     )
    ) -- as prer_ap_tmp     --------------------------- Prerekvizity a jejich nazvy (pro finalni vypis)
    
--select tmp.* from prer_ap_tmp tmp
--/*

    --------------------------- Dohledani lonskych rocniku pro cele ropg
    -- Hleda se jak pres obor.obor_lonsky_id, tak pres zmenu oboru (volbu oboru)
    , rocnik_lonsky_tmp as (select distinct -- Distinct odstranuje zdvojeni nepovinne volby oboru
            pg06.pg, pg06.ob, pg06.obor_id, pg06.rocnik, pg06.rok
            , pg06.rocnik_id
             -- Pokud neexistuje primy predchudce, hleda se ve volbe oboru
            , nvl(rl06.cislo_rocniku, rz06.cislo_rocniku) as rocnik_lonsky
            , nvl(rl06.rocnik_id, zo06.rocnik_puvodni_id) as rocnik_lonsky_id
            , nvl(rl06.obor_id, zo06.obor_puvodni_id) as obor_lonsky_id
            -- , nvl(pl06.rok, pz06.rok) as rok_lonsky
        from ropg pg06                                                                  -- *** Ropg
            left join st01.obor ol06                                                    -- *** Obor - lonsky
                  on ( ol06.obor_id = pg06.obor_lonsky_id
                       and ol06.status = 9 ) 
            left join st01.rocnik rl06                                                  -- *** Rocnik - lonsky
                  on ( rl06.obor_id = ol06.obor_id
                       and rl06.cislo_rocniku in (pg06.rocnik - 1, 0) -- Vcetne lonskeho nulteho rocniku (FIT, FEKT...)
                       and rl06.status = 9 ) 
--            left join ropg pl06                                                         -- *** Ropg - lonsky
--                 on pl06.rocnik_id = rl06.rocnik_id
            left join st01.zmena_oboru zo06                                             -- *** Zmena oboru - take lonsky rocnik
                  on ( zo06.rocnik_novy_id = pg06.rocnik_id
                       and zo06.status = 9 ) 
            left join st01.rocnik rz06                                                  -- *** Rocnik - lonsky ze zmeny oboru
                  on ( rz06.obor_id = zo06.obor_puvodni_id
                       and rz06.cislo_rocniku in (pg06.rocnik - 1, 0) -- Vcetne lonskeho nulteho rocniku (FIT, FEKT...)
                       and rz06.status = 9 ) 
--            left join ropg pz06                                                         -- *** Ropg - lonsky ze zmeny oboru
--                 on pz06.rocnik_id = rz06.rocnik_id
                       
    ) -- as rocnik_lonsky_tmp     --------------------------- Dohledani lonskych rocniku pro cele ropg
    
--select tmp.* from rocnik_lonsky_tmp tmp
--order by rocnik_id
--/*

--
--
    --------------------------- Zkoumane rocniky s prerekvizitou a jejich lonske rocniky
    -- V aktualnim roce, vychozi bod pro rekurzivni dohledavani predchozich rocniku
    , predchudci_src as ( select 
            -- Pro ladeni
            rt06.pg, rt06.ob, rt06.obor_id, rt06.rok, rt06.rocnik
            , rt06.rocnik_id
            , rt06.rocnik_lonsky, rt06.rocnik_lonsky_id, rt06.obor_lonsky_id
        from ( select distinct p06.rocnik_id 
               from pr_planu_sel_act_tmp p06                                            -- *** Pr_planu_sel_act_tmp
                    inner join predm_prer_tmp pre06                                     -- *** Predm_prer_tmp - filr na plany s pr. s prer.
                          on ( pre06.predmet_id = p06.predmet_id 
                             )
--               ---- Testovaci data na ozkouseni rekurze, pro rok 2021/22
--               union select 37860 from dual
--               union select 37869 from dual
              ) ro06
            inner join rocnik_lonsky_tmp rt06                                           -- *** Rocnik_lonsky_tmp
                  on rt06.rocnik_id = ro06.rocnik_id
    ) -- as predchudci_src     --------------------------- Zkoumane rocniky s prerekvizitou a jejich lonske rocniky
    
--select tmp.* from predchudci_src tmp
--/*

    --------------------------- Rekurzivne dohledani predchudci zkoumanych rocniku s prerekvizitou
    -- Rocnik (cislo rocniku), rocnik_id - rocnik, ve kterem je hlavni predmet, co ma prerekvizitu
    -- Rocnik_lonsky (cislo rocniku), rocnik_lonsky_id - rocnik, ve kterem se hleda prerekvizita/korekvizita
    , predchudci_tmp (pg, ob, obor_id, rok, rocnik, rocnik_id, rocnik_lonsky
                      , rocnik_lonsky_id, obor_lonsky_id) as ( 
        select pg, ob, obor_id, rok, rocnik, rocnik_id, rocnik_lonsky
               , rocnik_lonsky_id, obor_lonsky_id
        from predchudci_src                                                             -- *** Predchudci_src
        where rocnik_lonsky_id is not null -- Pouze pokud ma skutecne predchudce 
        -- Rekurzivni predchudci
        union all
        select p07.pg, p07.ob, p07.obor_id, p07.rok, p07.rocnik, p07.rocnik_id
            , nvl(rt07.rocnik_lonsky, -1), rt07.rocnik_lonsky_id, rt07.obor_lonsky_id
        from predchudci_tmp p07                                                         -- *** Predchudci_tmp - rekurze
            inner join rocnik_lonsky_tmp rt07                                           -- *** Rocnik_lonsky_tmp
                  on ( rt07.rocnik_id = p07.rocnik_lonsky_id
                       and rt07.rocnik_lonsky_id is not null -- Pouze pokud ma skutecne predchudce
                     )
        
    ) -- as predchudci_tmp     --------------------------- Rekurzivne dohledani predchudci zkoumanych rocniku s prerekvizitou
    
--select tmp.* from predchudci_tmp tmp
--order by pg, ob, rok, rocnik, rocnik_lonsky
--/*

    --------------------------- Rocniky, ve kterych se budou hledat plany s prerekvizitou/korekvizitou
    -- Vcetne hlavniho rocniku, ktery lze pouzit pouze s korekvizitou (priznak prerekvizita=0)
    -- Rocnik (cislo rocniku), rocnik_id - rocnik, ve kterem je hlavni predmet, co ma prerekvizitu
    -- Rocnik_lonsky (cislo rocniku), rocnik_lonsky_id - rocnik, ve kterem se hleda prerekvizita/korekvizita
    , rocniky_planu_tmp as (
        ---- Rekurzivne dohledani predchudci zkoumanych rocniku, vcetne nultych rocniku predchozich let
        select t08.*, 1 as prerekvizita -- Mozno pouzit s prerekvizitou (skutecni predchudci z lonskych let)
        from predchudci_tmp t08                                                         -- *** Predchudci_tmp
        union all
        ---- Vazba zkoumany rocnik sam na sebe - pro pripad korekvizity
        select pg, ob, obor_id, rok, rocnik, rocnik_id
            , rocnik as rocnik_lonsky, rocnik_id as rocnik_lonsky_id
            , obor_id as obor_lonsky_id
            , 0 as prerekvizita -- NE-mozno pouzit s prerekvizitou (pouze s korekvizitou)
        from predchudci_src s08                                                         -- *** Predchudci_src
        union all
        ---- Letosni nulty rocnik zkoumaneho obor_id - pro pripad korekvizity
        select z08.pg, z08.ob, z08.obor_id, z08.rok, z08.rocnik, z08.rocnik_id
            , rz08.cislo_rocniku as rocnik_lonsky, rz08.rocnik_id as rocnik_lonsky_id
            , rz08.obor_id as obor_lonsky_id
            , 0 as prerekvizita -- NE-mozno pouzit s prerekvizitou (pouze s korekvizitou)
        from predchudci_src z08                                                         -- *** Predchudci_src
            inner join st01.rocnik rz08                                                 -- *** Rocnik - nulty stejneho oboru a roku
              on ( rz08.obor_id = z08.obor_id
                   and rz08.cislo_rocniku = 0 -- Letosni nulty rocnik (FIT, FEKT...)
                   and rz08.status = 9 ) 
    ) -- as rocniky_planu_tmp     --------------------------- Rocniky, ve kterych se budou hledat plany s prerekvizitou/korekvizitou
    
--select tmp.* from rocniky_planu_tmp tmp
--order by pg, ob, rok, rocnik, rocnik_lonsky
--/*

    --------------------------- Studijni plany zkoumanych roku, bez jazykovych verzi
    -- Doplneni planu_act o predchozi rocniky, zkopirovano CTRL-C, CTRL-V, jen jine
    -- vychozi rocniky a dohledani udaju v ob25 a pg25
    , plany_all_raw as (select ro25.rocnik_id, pp25.aktualni_predmet_id
            , pp25.predmet_planu_id, pp25.povinnost, pp25.zmena_jazyka
            , ap25.predmet_id, pr25.zkratka as pr, pr25.fakulta_id as pr_fakulta_id
            , pg25.fakulta_id as pp_fakulta_id, ob25.ustav_id as pp_ustav_id
            , ap25.ustav_id as ap_ustav_id
            , pg25.rok_platnosti as rok
            , se25.typ_semestru_id
        from -- Vyber vsech zkoumanych rocniku
            ( select distinct rocnik_id, obor_id
              from rocniky_planu_tmp                                                    -- *** Rocniky_planu_tmp - aktualni
              union
              select distinct rocnik_lonsky_id, obor_lonsky_id
              from rocniky_planu_tmp                                                    -- *** Rocniky_planu_tmp - stare
            ) ro25
            inner join st01.obor ob25                                                   -- *** Obor
                  on ( ob25.obor_id = ro25.obor_id
                       and ob25.status = 9 )
            inner join st01.program pg25                                                -- *** Program
                  on ( pg25.program_id = ob25.program_id
                       and pg25.status = 9 )
            ---- Odtud nize je to kopie z plany_act_raw           
            inner join st01.semestr se25                                                -- *** Semestr 
                  on ( se25.rocnik_id = ro25.rocnik_id
                       and se25.status = 9 )
            inner join st01.predmet_planu pp25                                          -- *** Predmet_planu 
                  on ( pp25.semestr_id = se25.semestr_id
                       -- and pp25.povinnost in (2, 13)      -- 2-P, 13-PV
                       and pp25.status = 9 )
            inner join st01.aktualni_predmet ap25                                       -- *** Aktualni predmet
                  on ( ap25.aktualni_predmet_id = pp25.aktualni_predmet_id
                       and ap25.status = 9
                     )
            inner join st01.predmet pr25                                                -- *** Predmet
                  on ( pr25.predmet_id = ap25.predmet_id  
                       and pr25.status = 9
                      )
    ) -- as plany_all_raw     --------------------------- Studijni plany zkoumanych roku, bez jazykovych verzi

--select tmp.* from plany_all_raw tmp
----where predmet_planu_id = 616953
--order by rok desc, rocnik_id, povinnost, pr
--/*

    --------------------------- Studijni plany pouze jazykovych verzi, mozne zdvojeni kvuli akt. predm. 
    , plany_all_ekv_tmp as (select pn25.rocnik_id, apek25.aktualni_predmet_id
            , pn25.predmet_planu_id, pn25.povinnost, pn25.zmena_jazyka
            , prek25.predmet_id as predmet_id, prek25.zkratka as pr
            , prek25.fakulta_id as pr_fakulta_id
            , pn25.pp_fakulta_id, pn25.pp_ustav_id
            , apek25.ustav_id as ap_ustav_id
            , pn25.rok, pn25.typ_semestru_id
            , row_number() over ( partition by pn25.predmet_planu_id, prek25.predmet_id 
                                  order by decode(apek25.typ_semestru_id, pn25.typ_semestru_id,0, 1)
                                        , apek25.typ_semestru_id
                                ) as rn 
            -- , 1 as jazykova_erze
            -- Pro ladeni
--            , pr25.predmet_id, pr25.zkratka as pr, pr25.fakulta_id as pr_fakulta_id
--            , prek25.zkratka as pr_ek
--            , apek25.typ_semestru_id as apek25_typ_semestru_id
        from plany_all_raw pn25                                                         -- *** Plany_all_raw
            inner join st01.ekvivalentni_predmety eps25                                 -- *** Ekvivalentni_predmety - skupina
                  on ( eps25.predmet_id = pn25.predmet_id
                       and pn25.zmena_jazyka = 1 -- Pouze tam, kde je povolena zmena jazyka
                       and eps25.status = 9 ) 
            inner join st01.ekvivalentni_predmety ep25                                  -- *** Ekvivalentni predmety - dalsi predmet
                  on ( ep25.skupina_id = eps25.skupina_id 
                       and ep25.predmet_id != pn25.predmet_id -- Bez orig. predmetu
                       and ep25.status = 9
                     )
            inner join st01.predmet prek25                                              -- *** Predmet - ekvivalentni predmet
                  on ( prek25.predmet_id = ep25.predmet_id
                       and prek25.status = 9
                     )
            inner join st01.aktualni_predmet apek25                                     -- *** Aktualni_predmet - ekvivalentni predmet
                  on ( apek25.predmet_id = prek25.predmet_id
                       and apek25.rok = pn25.rok     -- Ekvivalentni akt. predmet musi byt ve stejnem roce, semestr se filtruje pomoci rn
                       and apek25.status = 9
                     )
    ) -- as plany_all_ekv_tmp     --------------------------- Studijni plany pouze jazykovych verzi, mozne zdvojeni kvuli akt. predm. 
    
--select tmp.* from plany_all_ekv_tmp tmp
--order by rocnik_id, predmet_planu_id, pr
--/*

            
    --------------------------- Studijni plany zkoumanych roku vcetne jazykovych verzi
    , plany_all_tmp as (
        select pw25.*, 0 as jazykova_verze 
        from plany_all_raw pw25                                                         -- *** Plany_all_raw
        union all
        select pe25.*
            -- , 1 as jazykova_verze - Misto 1 (jazykova verze) je tu sloupec rn = 1 ;-)
        from plany_all_ekv_tmp pe25                                                     -- *** Plany_all_ekv_tmp
        where pe25.rn = 1 -- Pouze nejvhodnejsi akt. predmet daneho roku (nejlepe stejny semestr)
     
    ) -- as plany_all_tmp     --------------------------- Studijni plany zkoumanych roku vcetne jazykovych verzi
    
--select tmp.* from plany_all_tmp tmp
----where predmet_planu_id = 616953
----where rocnik_id = 35434
--order by rok desc, rocnik_id, povinnost, pr
--/*


    --------------------------- Predmety s prerekvizitou/korekvizitou a rocniky predmetu
    -- K hlavnimu
    , predm_pl_tmp as (select pre11.*
--            pre11.predmet_id, pre11.pr, pre11.pr_fakulta_id, pre11.typ_prerekvizity_id
--            , pre11.prer_predmet_id, pre11.prer_pr, pre11.prer_fakulta_id, pre11.info
            , p11.zmena_jazyka as predm_zm_jaz, p11.jazykova_verze as predm_j_verze
            , p11.aktualni_predmet_id, p11.predmet_planu_id
            , p11.rocnik_id, p11.povinnost, p11.typ_semestru_id
        from plany_act_tmp p11                                                          -- *** Plany_act_tmp
            inner join predm_prer_tmp pre11                                             -- *** Predm_prer_tmp 
                  on pre11.predmet_id = p11.predmet_id
    ) -- as predm_pl_tmp     --------------------------- Predmety s prerekvizitou/korekvizitou a rocniky predmetu
    
--select tmp.* from predm_pl_tmp tmp
--order by rocnik_id, pr, prer_pr
--/*


    --------------------------- Prerekvizity a predch. rocniky, kde se vyskytuji
    -- Neresi se samotna prerekvizita, ale prerekvizita a jeji ekvivalentni a uznatelne predmety
    , prer_ro_tmp as (select 
            -- Predmet s prerekvizitou
            ro12.rocnik_id, ro12.predmet_id, ro12.povinnost 
            , ro12.min_hodnoceni
            -- Predmet prerekvizity
            , ro12.prer_predmet_id, pe12.ekv_predmet_id
            , pp12.povinnost as prer_povinnost
            , rp12.rocnik_lonsky as prer_rocnik
            , rp12.rocnik_lonsky || '. r. (sem.: ' || cts12.zkratka 
                 || ', pov.: ' || cp12.povinnost_k_sp as prer_ro_pov -- Pro vypis
            , rp12.rocnik_lonsky_id as prer_rocnik_id
            , rp12.obor_lonsky_id as prer_obor_id
            -- , pp12.aktualni_predmet_id as prer_ap_id
            , pp12.aktualni_predmet_id as ekv_ap_id
            -- Odklad_predmetu - priznak, zda nesplneni prerekvizity (korekvizity)
            -- v tomto rocniku muze zpusobit odklad hlavniho predmetu na dalsi ak. rok.
            --     Nastava, kdyz prerekvizita je v predchozim ak. roce, nebo
            -- kdyz je korekvizita ve stejnem ak. roce v predchozim semestru
            -- a student si musi od-zapsat predmet, pokud korekvizitu nesplni
            , ( case
                  when ro12.typ_prerekvizity_id = 1 -- 1-pov. prer.
                       and pp12.rok = rp12.rok - 1  -- Predchozi ak. rok (=skutecne lonsky) 
                    then 1
                  when ro12.typ_prerekvizity_id = 2 -- 2-pov. kor.
                       and ro12.typ_semestru_id = 1 -- 1-letni, semestr hlavniho predmetu
                       and pp12.typ_semestru_id = 2 -- 2-zimni, semestr prerekvizity
                       and pp12.rok = rp12.rok -- Stejny ak. rok 
                    then 1 -- Vede k nucenemu odlozeni predmetu pokud se mezi semestry od-zapisuje hlavni predmet
                  else 0
               end ) as odklad_predmetu 
            
            -- Pro ladeni, pouziva se v ladicich vypisech i v nasledujicich castech
            , ro12.pr, pp12.pr as prer_pr, pe12.ekv_pr
            , pp12.zmena_jazyka as prer_zm_jaz, pp12.jazykova_verze as prer_j_verze
            , pp12.rok as prer_rok
--            , ro12.typ_prerekvizity_id, rp12.prerekvizita
--            , rp12.rok as rp12_rok_predmetu
        from predm_pl_tmp ro12                                                          -- *** Predm_pl_tmp - predmety a jejich prerekvizity
            inner join rocniky_planu_tmp rp12                                           -- *** Rocniky_planu_tmp - plan predmetu
                  on ( rp12.rocnik_id = ro12.rocnik_id
                       and ( ro12.typ_prerekvizity_id = 2       -- 1-pov. prer., 2-pov. kor. -- Korekvizita s jakymkoli rocnikem
                             or ( ro12.typ_prerekvizity_id = 1  -- 1-pov. prer., 2-pov. kor.
                                  and rp12.prerekvizita = 1     -- Prerekvizita pouze s rocnikem pro prerekvizity
                                )
                           )
                     )
            inner join ropg ar12                                                        -- *** Ropg - lonsky
                 on ar12.rocnik_id = rp12.rocnik_id
            inner join prer_ekv_tmp pe12                                                -- *** Prer_ekv_tmp - ekvivalenty k prerekvizitam
                  on pe12.prer_predmet_id = ro12.prer_predmet_id

            inner join plany_all_tmp pp12                                               -- *** Plany_all_tmp - prerekvizita
                  on ( pp12.rocnik_id = rp12.rocnik_lonsky_id
                       -- and pp12.predmet_id = ro12.prer_predmet_id -- Dohledavani prerekvizit primo
                       and pp12.predmet_id = pe12.ekv_predmet_id    -- Dohledavani ekvivalentu k prerekvizite
                     )  
            inner join st01.c_povinnost_ke_sp cp12                                      -- *** C_povinnost_ke_sp - povinnost prerekvizity
                  on ( cp12.cislo_povinnosti = pp12.povinnost
                       -- and cp12.povinnost_k_sp = 'P'
                       ) 
            inner join st01.c_typ_semestru cts12                                        -- *** C_typ_semestru 
                  on ( cts12.typ_semestru_id = pp12.typ_semestru_id
                     )
    ) -- as prer_ro_tmp     ---------------------------  Prerekvizity a predch. rocniky, kde se vyskytuji
    
--select tmp.* from prer_ro_tmp tmp
----where tmp.rocnik_id = 37856
--order by rocnik_id, pr, prer_pr, ekv_pr, prer_j_verze
--/*

    --------------------------- Prerekvizity v predch. rocniku a jejich studenti dle rocniku predmetu
    , prer_stud_stat_tmp as (select 
            pr13.rocnik_id, pr13.predmet_id, pr13.prer_predmet_id, pr13.ekv_predmet_id
            , pr13.prer_rocnik_id, pr13.prer_obor_id
            , pr13.ekv_ap_id, ap13.nazev as ekv_predmet
            , pr13.odklad_predmetu -- Priznak, ze se kvuli nesplnene prerekvizite odklada predmet
            , count(ei13.el_index_id) as poc_studentu -- Zapsani studenti
            , nvl( sum( case
                          when pr13.min_hodnoceni is null   -- Min. hodnoceni se nehlida
                            then ei13.absolvoval            -- Pocita se jen absolvovani
                          when nvl(ei13.znamka,1) <= pr13.min_hodnoceni -- U hodnoceni bez znamky se kontrola na min. hodnoceni vyrazuje
                            then ei13.absolvoval -- Pocita se absolvovani
                          else 0 -- Neabsolvoval i v pripade, 
                        end 
                      ), 0 
                   ) as poc_abs
--            ---- Pro testovani s vypnutym "group by", zda to vraci spravne studenty rocniku
--            , pr13.pr, pr13.prer_pr, pr13.prer_rok
--            , ip13.individualni_plan_id, ei13.el_index_id, ei13.absolvoval
--            , s.studium_id, pe.*
--            , sropg.pg, sropg.ob, sropg.rocnik as ro
        from prer_ro_tmp pr13                                                           -- *** Prer_ro_tmp
            inner join st01.aktualni_predmet ap13                                       -- *** Aktualni predmet - ekvivalent prerekvizity
                  on ( ap13.aktualni_predmet_id = pr13.ekv_ap_id
                       and ap13.status = 9
                     )
            left join st01.individualni_plan ip13                                       -- *** Individualni_plan - prerekvizita
                  on ( ip13.aktualni_predmet_id = pr13.ekv_ap_id
                       and ip13.zaregistrovat = 1           -- Zaregistrovane a neodregistrovane predmety
                       and ip13.el_index_id is not null     -- Pouze zapsane predmety
                       and ip13.status = 9
                       ---- Studenti, kteri studuji v rocniku prerekvizity (nebo v oboru prerekvizity, kt. je v 0. rocniku) 
                       and ( ( pr13.prer_rocnik = 0 -- Prerekvizita vlozena v 0. rocniku
                               and exists           -- Student studuje stejny obor_id 
                               ( select zs14.rocnik_id
                                 from st01.zmena_studia zs14                            -- *** Zmena_studia
                                      inner join st01.rocnik ro14                       -- *** Rocnik 
                                            on ( ro14.rocnik_id = zs14.rocnik_id
                                                 and zs14.aktualni_pro_rok = 1 -- Pouze posledni zmena studia v roce
                                                 and ro14.status = 9 and zs14.status = 9 
                                               )
                                 where zs14.studium_id = ip13.studium_id -- Stejny student
                                    and ro14.obor_id = pr13.prer_obor_id -- Student zapsany v rocniku prerekvizity
                               )
                             )
                             or exists              -- Student studuje v rocniku prerekvizity
                               ( select zs14.rocnik_id
                                 from st01.zmena_studia zs14                            -- *** Zmena_studia
                                 where zs14.studium_id = ip13.studium_id -- Stejny student
                                    and zs14.rocnik_id = pr13.prer_rocnik_id -- Student zapsany v rocniku prerekvizity
                                    and zs14.aktualni_pro_rok = 1 -- Pouze posledni zmena studia v roce
                                    and zs14.status = 9
                               )
                           )
                      )
            left join st01.el_index ei13                                                -- *** El_index 
                  on ( ei13.el_index_id = ip13.el_index_id
                       and ei13.aktualni_predmet_id = pr13.ekv_ap_id -- Stejny zapsany predmet
                       and ei13.status = 9 )
                       
--            ---- Pro testovani, zda to vraci spravne studenty rocniku
--            left join st01.studium s                                                    -- *** Studium
--                  on ( s.studium_id = ei13.studium_id
--                       and s.status = 9                 
--                      )    
--            left join st01.zmena_studia zs                                              -- *** Zmena studia
--                  on ( -- zs.zmena_studia_id = s.posledni_zmena_studia_id
--                       zs.studium_id = s.studium_id -- Vazba pro aktualni_pro_rok
--                       and zs.skolni_rok = pr13.prer_rok -- Stejny rok studia studenta jako prerekvizity v planu
--                       and zs.aktualni_pro_rok = 1
--                       -- and zs.stav_studia_id < 20                    -- zapsani studenti 
--                       -- and zs.stav_studia_id > 1 
--                       and zs.status = 9                 
--                      )
--            left join (select pe50.per_id --, pe50.per_family_names as prijmeni, pe50.per_first_names as jmeno 
--                        , pe50.label_pr as student 
--                                   from brutisadm.person pe50                           -- *** Person
--                                   where pe50.status = 9 ) pe
--                   on pe.per_id = s.student_id
--            left join ropg sropg                                                        -- *** RoPg
--                 on sropg.rocnik_id = zs.rocnik_id
        group by pr13.rocnik_id, pr13.predmet_id, pr13.prer_predmet_id, pr13.ekv_predmet_id
            , pr13.prer_rocnik_id, pr13.prer_obor_id, pr13.ekv_ap_id
            , ap13.nazev, pr13.odklad_predmetu
    ) -- as prer_stud_stat_tmp     --------------------------- Prerekvizity v predch. rocniku a jejich studenti dle rocniku predmetu
    
--select tmp.* from prer_stud_stat_tmp tmp
----where prer_pr = 'A5'
----    and pr = 'A6'
----    and ob = 'B-MET'
----    and ro = 2
----order by pg, ob, ro, absolvoval, student
--order by rocnik_id, predmet_id, prer_predmet_id, prer_rocnik_id, ekv_predmet_id, ekv_ap_id
--/*


    --------------------------- Plneni prerekvizit - rocniky, ve kterych se vyskytuje
    , prer_plneni_tmp as (select pro15.rocnik_id, pro15.predmet_id
            , pro15.prer_predmet_id, pro15.ekv_predmet_id, pro15.ekv_pr
            , max(pro15.prer_zm_jaz) as prer_zm_jaz
            , max(pro15.prer_j_verze) as prer_j_verze
            , max( case
                      when pro15.prer_povinnost = 2 -- 2-P, povinna prerekvizita je OK
                        then 1
                      when pro15.povinnost = pro15.prer_povinnost -- Stejna povinnost predmetu i prerekvizity je OK
                        then 1
                      when pro15.povinnost = 5 -- 5-V, volitelny predmet je OK (nemusi si ho vybrat)
                        then 1
                      else 0 -- Ostatni kombinace povinnosti jsou spatne
                   end ) as povinnost_kontrola
            -- Pocet studentu prerekvizity, pokud budou odkladat predmet
            , max(rs15.odklad_predmetu) as odklad_predmetu -- Samotny priznak
            , sum(decode(rs15.odklad_predmetu, 1,rs15.poc_studentu, 0)) as odklad_studentu
            , sum(decode(rs15.odklad_predmetu, 1,rs15.poc_abs, 0)) as odklad_abs
            , sum(decode(rs15.odklad_predmetu, 1,(rs15.poc_studentu - rs15.poc_abs), 0)) as odklad_neabs
            -- Celkovy pocet studentu prerekvizity napric predchozimi rocniky
            , sum(rs15.poc_studentu) as celk_studentu
            , sum(rs15.poc_abs) as celk_abs
            , sum(rs15.poc_studentu - rs15.poc_abs) as celk_neabs 
            , listagg(distinct pro15.prer_ro_pov || '; zaps.: ' || rs15.poc_studentu 
                            || ', abs.: ' || rs15.poc_abs|| ', neabs.: ' || (rs15.poc_studentu - rs15.poc_abs)
                            || decode(nvl(rs15.poc_studentu, 0)
                                            , 0,null
                                            , ' (' 
                                              || round(100*(rs15.poc_studentu - rs15.poc_abs)/rs15.poc_studentu, 0) 
                                              || '%)'
                                         ) 
                            || ')'
                        ,', '||chr(13)||chr(10)) 
                    within group(order by pro15.prer_ro_pov) as prer_rocniky       -- Vycet rocniku a zkratek povinnosti prerekvizit
            , listagg(distinct pro15.prer_rocnik_id,', ') 
                    within group(order by pro15.prer_rocnik_id) as prer_rocniky_id  -- Vycet rocniku_id prerekvizit
            , listagg(distinct pro15.ekv_ap_id,', ') 
                    within group(order by pro15.ekv_ap_id) as ekv_ap_id  -- Vycet aktualni_predmet_id prerekvizit
            , listagg(distinct rs15.ekv_predmet,', ') 
                    within group(order by rs15.ekv_ap_id) as ekv_predmety  -- Vycet nazvu prerekvizit
--            -- Pro ladeni
--            , rp15.rocnik_lonsky as prer_rocnik
--            , rp15.rocnik_lonsky_id as prer_rocnik_id
--            , pp15.pr as prer_pr, pp15.rok as prer_rok, pp15.povinnost as prer_povinnost
--            , ro15.typ_prerekvizity_id, rp15.prerekvizita
        from prer_ro_tmp pro15                                                          -- *** Prer_ro_tmp
            left join prer_stud_stat_tmp rs15                                           -- *** Prer_stud_stat_tmp
                 on ( rs15.rocnik_id = pro15.rocnik_id
                      and rs15.predmet_id = pro15.predmet_id
                      and rs15.prer_rocnik_id = pro15.prer_rocnik_id
                      and rs15.ekv_ap_id = pro15.ekv_ap_id
                    )
        group by pro15.rocnik_id, pro15.predmet_id, pro15.prer_predmet_id
            , pro15.ekv_predmet_id, pro15.ekv_pr
    ) -- as prer_plneni_tmp     --------------------------- Plneni prerekvizit - rocniky, ve kterych se vyskytuje
    
--select tmp.* 
--    , pr.zkratka as tmp_only_predm
--from prer_plneni_tmp tmp
--    inner join st01.predmet pr                                                          -- *** Predmet
--          on ( pr.predmet_id = tmp.predmet_id  
--               -- and pr.fakulta_id = 4 
--               -- and pr.zkratka like '9%'
--               and pr.status = 9
--              )
----where tmp.rocnik_id = 35434 -- 35434 - B-MET, pr.: A6, kor.: A5
--order by rocnik_id, pr.zkratka, ekv_pr
--/*



    --------------------------- Finalni prehled
    , prehled_tmp as (select 
            pro16.pr_fakulta_id, orgp16.org_acronym as pr_fak
            , orgu16.org_acronym as pr_ust, otu16.orgunit_name as pr_ustav
            , pro16.predmet_id, pro16.aktualni_predmet_id, pro16.predmet_planu_id
            , pro16.pr, ap16.nazev as predmet
            , pro16.predm_zm_jaz, pro16.predm_j_verze -- Zmena jazyka povolena; predmet je odlisna jaz. verze
            
            , an16.prerekvizity_cz as prerekvizity_anotace
            -- Nastaveni prerekvizity
            , pro16.prerekvizita_id, pro16.typ_prerekvizity_id
            , ct16.zkratka as typ_prer, ct16.popis as typ_prerekvizity
            , pro16.min_hodnoceni, pro16.info
            , row_number() over ( partition by pro16.prerekvizita_id 
                                  order by ro16.typ, ro16.pg, ro16.ob, ro16.zam
                                        , pl16.ekv_pr) as prerekvizita_poradi 
            -- Predmet prerekvizity
            , pro16.prer_predmet_id
            , pl16.ekv_predmet_id, nvl(pl16.ekv_ap_id, apr16.prer_ap_id) as ekv_ap_id
            , orgur16.org_acronym as prer_ust, otur16.orgunit_name as prer_ustav
            , pro16.prer_fakulta_id, orgr16.org_acronym as prer_fak
            , pro16.prer_pr, pl16.ekv_pr
            , nvl(pl16.ekv_predmety, apr16.prer_predmet) as ekv_predmety
            , pl16.prer_zm_jaz, pl16.prer_j_verze
            -- Vyhodnoceni prerekvizity
            , decode(pl16.rocnik_id, null,0, 1) as prer_nalezena
            , decode(pl16.rocnik_id, null,95, 96) as prer_nalezena_ico --96 - zelena fajfka, 95 - cerveny krizek
            , pl16.povinnost_kontrola
            , decode(pl16.povinnost_kontrola, null,-1, 1,169, 168) as povinnost_kontrola_ico -- 169 - zeleny puntik, 168 - cerveny puntik
            , pl16.odklad_predmetu
            , pl16.odklad_studentu, pl16.odklad_abs, pl16.odklad_neabs
            , to_number(decode(nvl(pl16.odklad_studentu, 0)
                        , 0,null
                        , round(100*pl16.odklad_neabs/pl16.odklad_studentu, 0)
                     )) as odklad_neabs_proc
            , pl16.celk_studentu, pl16.celk_abs, pl16.celk_neabs
            , to_number(decode(nvl(pl16.celk_studentu, 0)
                        , 0,null
                        , round(100*pl16.celk_neabs/pl16.celk_studentu, 0)
                     )) as celk_neabs_proc
            , pl16.prer_rocniky, pl16.prer_rocniky_id
            
            -- Zarazeni hlavniho predmetu, ktery ma prerekvizitu
            , ro16.program_id, ro16.obor_id, pro16.rocnik_id
            , cp16.povinnost_k_sp as pr_pov, cp16.popis as pr_povinnost, pro16.povinnost
            , pro16.typ_semestru_id, cts16.zkratka as sem, cts16.popis as semestr
            
            , ro16.typ, ro16.forma as fo, ro16.delka_studia as del
            , ro16.stupen, ro16.rocnik
            , ro16.pg, ro16.program, ro16.ob, ro16.obor
            , ro16.zam, ro16.zamereni
            , ro16.jaz, ro16.spol_pg
            , org16.org_acronym as ust_oboru, ot16.orgunit_name as ustav_oboru
            , ro16.garant_id, pe16.garant
            , orgf16.org_acronym as fak_programu
            , ro16.rok
            ---- Seskupeni predmetu do skupin (pro barveni radku) 
--            , dense_rank()
--                    over ( order by orgp16.org_acronym, orgu16.org_acronym, pro16.pr )
--                    as grp_id -- Neustale rostouci ID, netreba
            , mod(dense_rank()
                        over ( order by orgp16.org_acronym, orgu16.org_acronym, pro16.pr )
                   , 10) as grp_id_mod -- Cyklicky opakujici se ID pro barvu
        from predm_pl_tmp pro16                                                         -- *** Predm_pl_tmp - predmety s prerekvizitou a rocniky
            inner join st01.aktualni_predmet ap16                                       -- *** Aktualni predmet - predmet s prerekvizitou
                  on ( ap16.aktualni_predmet_id = pro16.aktualni_predmet_id
                       and ap16.status = 9
                     )
            inner join st01.c_povinnost_ke_sp cp16                                      -- *** C_povinnost_ke_sp - predmet s prerekvizitou
                  on ( cp16.cislo_povinnosti = pro16.povinnost
                       -- and cp16.povinnost_k_sp = 'P'
                       ) 
            inner join ropg ro16                                                        -- *** Ropg - predmet s prerekvizitou
                  on ( ro16.rocnik_id = pro16.rocnik_id
                     )
            inner join st01.c_typ_prerekvizity ct16                                     -- *** C_typ_prerekvizity
                  on ( ct16.typ_prerekvizity_id = pro16.typ_prerekvizity_id
                       )
            inner join st01.c_typ_semestru cts16                                        -- *** C_typ_semestru 
                  on ( cts16.typ_semestru_id = pro16.typ_semestru_id
                       and cts16.status = 9 )    
           
            left join prer_plneni_tmp pl16                                              -- *** Prer_plneni_tmp
                 on ( pl16.rocnik_id = pro16.rocnik_id
                      and pl16.predmet_id = pro16.predmet_id
                      and pl16.prer_predmet_id = pro16.prer_predmet_id
                     )
            left join prer_ap_tmp apr16                                                 -- *** Prer_ap_tmp
                 on ( apr16.prer_predmet_id = pro16.prer_predmet_id
                      and apr16.rn = 1 -- Pouze nejnovejsi zaznam
                    )
            left join st01.anotace an16                                                 -- *** Anotace - text prerekvizity
                  on ( an16.aktualni_predmet_id = pro16.aktualni_predmet_id
                       and an16.status = 9 ) 
            
            left join (select pe50.per_id --, pe50.per_family_names as prijmeni, pe50.per_first_names as jmeno 
                        , pe50.label_pr as garant 
                                   from brutisadm.person pe50                           -- *** Person - garant programu
                                   where pe50.status = 9 ) pe16
                   on pe16.per_id = ro16.garant_id
            
            left join brutisadm.orgunit org16                                           -- *** Orgunit - ustav oboru
                  on org16.orgunitid = ro16.ustav_id       
            left join brutisadm.orgunit_translation ot16                                -- *** Orgunit_translation - ustav oboru
                  on ( ot16.orgunitid = org16.orgunitid
                       and ot16.orgunit_trans_type = 'o'   -- Pro jednoduchost originalni nazev
                       and ot16.status = 9
                       and ot16.actual = 1
                      )
            left join brutisadm.orgunit orgu16                                          -- *** Orgunit - ustav predmetu
                  on orgu16.orgunitid = ap16.ustav_id
            left join brutisadm.orgunit_translation otu16                               -- *** Orgunit_translation - ustav predmetu
                  on ( otu16.orgunitid = orgu16.orgunitid
                       and otu16.orgunit_trans_type = 'o'   -- Pro jednoduchost originalni nazev
                       and otu16.status = 9
                       and otu16.actual = 1
                      )
            left join brutisadm.orgunit orgur16                                         -- *** Orgunit - ustav prerekvizity
                  on orgur16.orgunitid = apr16.prer_ustav_id
            left join brutisadm.orgunit_translation otur16                              -- *** Orgunit_translation - ustav prerekvizity
                  on ( otur16.orgunitid = orgur16.orgunitid
                       and otur16.orgunit_trans_type = 'o'   -- Pro jednoduchost originalni nazev
                       and otur16.status = 9
                       and otur16.actual = 1
                      )
            left join brutisadm.orgunit orgf16                                          -- *** Orgunit - fakulta programu
                  on orgf16.orgunitid = ro16.fakulta_id
            left join brutisadm.orgunit orgp16                                          -- *** Orgunit - fakulta predmetu
                  on orgp16.orgunitid = pro16.pr_fakulta_id
            left join brutisadm.orgunit orgr16                                          -- *** Orgunit - fakulta prerekvizity
                  on orgr16.orgunitid = pro16.prer_fakulta_id
    ) -- as prehled_tmp     --------------------------- Finalni prehled

--------------------------- Main select - finalni prehled + pridana barva radku podle predmetu
select bt.barva, tmp.* 
from prehled_tmp tmp
    left join barva bt
         on bt.barva_id2 = tmp.grp_id_mod
--where tmp.pr_ust = 'ÚK' or tmp.ust_oboru = 'ÚK'
----where povinnost_kontrola = 0
----where pr = '6KT'
----    and ob = 'B-MET'
----where prer_povinnosti_ke_sp like '%,%'
order by pr_fak, pr_ust, pr, typ, pg, ob, zam, prer_pr, ekv_pr
/*


-- */