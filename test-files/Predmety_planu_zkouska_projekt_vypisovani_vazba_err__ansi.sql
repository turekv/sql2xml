/* Vypisuje predmety, ktere :
   * nejsou ukoncene zkouskou a maji moznost vypisovat zkousku,
   * nejsou ukoncene kl a maji moznost vypisovat kl,
   * nejsou ukoncene zapoctem a maji moznost vypisovat zapocet,
   Toto je dusledek zmen planu a klonovani predmetu, opravit staci rucne.   
*/
with
    --------------------------- C_typ_ukonceni - povolene typy
    ctu_tmp as ( select ctu5.typ_ukonceni_predmetu_id, ctu5.zkratka
                        , ctu5.pozadavek_zapoctu, ctu5.pozadavek_zkousky
                 from st01.c_typ_ukonceni_predmetu ctu5                        -- *** C_typ_ukonceni_predmetu - akt. predmet
                 where ctu5.status = 9 
                     and ( ctu5.zkratka like '%zk%' 
                           or ctu5.zkratka like '%kl%'
                           or ctu5.zkratka like '%zá%'
                         )
                     and ctu5.pocitat_do_zapsanych = 1
                         
    ) -- as ctu_tmp     --------------------------- C_typ_ukonceni - povolene typy
    
--select tmp.* from ctu_tmp tmp
--/*
    --------------------------- Semestr - program
    , sepg_tmp as ( select semestr.semestr_id         -- Vraci udaje o studijnim planu, kam je predmet zarazen
                , pg.program_id, pg.program, cts.ty, cfs.fo, pg.de 
                , obor.nazev as obor, obor.zamereni as zam, rocnik.cislo_rocniku as ro
                , stupen.st
             from 
              (select program.program_id, program.fakulta_id     -- Vraci semestry FSI pro akt. rok
                      , program.nazev as program, program.typ_studia_id
                      , program.forma_studia_id, program.delka_studia as de
                    from st01.program 
                    where program.status = 9 
                           and program.fakulta_id = 4
                           and program.rok_platnosti = to_char(sysdate, 'YYYY') -- Rok
                          ) pg  
                inner join st01.obor
                         on ( obor.program_id = pg.program_id and obor.status = 9 )
                inner join (select stupen.stupen_id, stupen.obor_id, stupen.cislo_stupne as st
                                   from st01.stupen where stupen.status = 9) stupen
                        on stupen.obor_id = obor.obor_id
                inner join st01.rocnik
                         on ( rocnik.stupen_id = stupen.stupen_id and rocnik.status = 9 ) 
                inner join st01.semestr
                         on ( semestr.rocnik_id = rocnik.rocnik_id and semestr.status = 9 )
                inner join (select st01.c_typ_studia.typ_studia_id, st01.c_typ_studia.zkratka as ty
                            from st01.c_typ_studia where st01.c_typ_studia.status = 9) cts
                        on cts.typ_studia_id = pg.typ_studia_id
                inner join (select st01.c_forma_studia.forma_studia_id, st01.c_forma_studia.zkratka as fo
                            from st01.c_forma_studia where st01.c_forma_studia.status = 9) cfs
                on cfs.forma_studia_id = pg.forma_studia_id
    ) -- as sepg_tmp     --------------------------- Semestr - program
    
--select tmp.* from sepg_tmp tmp
--/*
    --------------------------- Predmety a aktualni predmety
    , pap_tmp as (select
             ap.aktualni_predmet_id
              , p.zkratka, ap.nazev, ap.povinnost, ap.kredity
              , ap.typ_ukonceni_predmetu_id
              , ap.rok
              --, ctua.zkratka as uk_ap
              , ( select listagg( ctp.zkratka , ', ') 
                        within group (order by ctp.zkratka)
                         as pozn
                    from 
                         st01.c_typ_ukonceni_predmetu ctp
                    where ctp.typ_ukonceni_predmetu_id = ap.typ_ukonceni_predmetu_id
                          or exists
                             ( select pp.predmet_planu_id
                               from st01.predmet_planu pp
                               where pp.status = 9
                                     and pp.aktualni_predmet_id = ap.aktualni_predmet_id
                                     and ctp.typ_ukonceni_predmetu_id = pp.typ_ukonceni_predmetu_id
                             )
                           ) as uk_ap_pp_all -- Vsechna ukonceni podle akt. predmetu i predmetu planu
        from
            (select predmet.* from st01.predmet                                         -- *** Predmet
                              where predmet.status = 9
                                    and predmet.fakulta_id = 4 
                                    and predmet.predmet_jine_vs = 0
                                    ) p
            inner join (select aktualni_predmet.* from st01.aktualni_predmet            -- *** Aktualni_predmet
                              where aktualni_predmet.status = 9
                                    and aktualni_predmet.rok = to_char(sysdate, 'YYYY')         -- Rok
                                    ) ap
                on ap.predmet_id = p.predmet_id
            left join ctu_tmp ctua  
                 on ctua.typ_ukonceni_predmetu_id = ap.typ_ukonceni_predmetu_id
    ) -- as pap_tmp     --------------------------- Predmety a aktualni predmety
    
--select tmp.* from pap_tmp tmp 
--/*    



select
     -- Dotazovaci cast dotazu
     pap.aktualni_predmet_id
      , pap.zkratka, pap.nazev, pap.rok
     , pap.uk_ap_pp_all
      , ( select listagg( cth.popis, ', ' ) 
               within group (order by cth.popis)
                as pozn
           from st01.zkouska_projekt zp                                                 -- *** zkouska_projekt
                inner join st01.c_typ_hodnoceni cth                                     -- *** c_typ_hodnoceni
                      on ( cth.typ_hodnoceni_id = zp.typ_atributu
                           -- and cth.status = 9  
                         )
           where zp.aktualni_predmet_id = pap.aktualni_predmet_id
                and zp.status = 9 ) as zkouska_projekt
     
from
    pap_tmp pap                                                                                     -- *** Vybrane predmety a aktualni predmety

--       left join (select ctu5.typ_ukonceni_predmetu_id, ctu5.zkratka as uk_ap
--                     from st01.c_typ_ukonceni_predmetu ctu5                        -- *** C_typ_ukonceni_predmetu - akt. predmet
--                         where ctu5.status = 9 
--                         and ( ctu5.zkratka like '%zk%' 
--                            or ctu5.zkratka like '%kl%'
--                          )
--                      ) ctua  
--              on ctua.typ_ukonceni_predmetu_id = ap.typ_ukonceni_predmetu_id
          
------------------------------------------------------------------------------------------          
where
     ( uk_ap_pp_all not like '%zk%'  
       and exists 
          ( select *
           from st01.zkouska_projekt zp
           where 
                    zp.status = 9
                 and zp.aktualni_predmet_id = pap.aktualni_predmet_id
                 and zp.typ_atributu in (5, 6, 7, 55) -- 5 - pisemna cast zk, 6 - ustni cast zk, 7 - zaverecna zkouska, 55 - testova cast zaverecne zkousky
          ) -- End of not exists
      )    
    or ( uk_ap_pp_all not like '%kl%'  
           and exists 
              ( select *
               from st01.zkouska_projekt zp
               where 
                        zp.status = 9
                     and zp.aktualni_predmet_id = pap.aktualni_predmet_id
                     and zp.typ_atributu = 9 -- 5 - pisemna cast zk, 6 - ustni cast zk, 7 - zaverecna zkouska, 9 - klas. zapocet 
              ) -- End of not exists
          )
    or ( uk_ap_pp_all not like '%zá%'  
           and exists 
              ( select *
               from st01.zkouska_projekt zp
               where 
                        zp.status = 9
                     and zp.aktualni_predmet_id = pap.aktualni_predmet_id
                     and zp.typ_atributu = 8 -- 5 - pisemna cast zk, 6 - ustni cast zk, 7 - zaverecna zkouska, 8 - zapocet, 9 - klas. zapocet 
              ) -- End of not exists
          )

        
order by 5

-- */
