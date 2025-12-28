Introduction
============

Apps Logging App è un framework Python progettato per la raccolta, il filtraggio
e la distribuzione di informazioni provenienti da diverse fonti applicative e
infrastrutturali.

Il sistema è stato concepito con l'obiettivo di essere scalabile e altamente
configurabile, mantenendo al contempo un insieme di percorsi di configurazione
ben definiti. Questo approccio consente di estendere il comportamento
dell'applicazione principalmente attraverso file di configurazione, riducendo
al minimo la necessità di modifiche al codice sorgente.

Apps Logging App consente la raccolta di log, parametri e informazioni strutturate
o semi-strutturate, provenienti da file di sistema, applicazioni locali o
interrogazioni verso sistemi di database esterni. Le informazioni raccolte
vengono successivamente elaborate da componenti dedicati, filtrate secondo
regole configurabili e infine inviate a sistemi di messaggistica asincrona
tramite produttori di messaggi.

L'architettura del sistema è basata su un modello a flussi configurabili, in cui
ogni flusso definisce in modo dichiarativo le sorgenti dei dati, le logiche di
elaborazione e i meccanismi di destinazione. Questo rende il framework
particolarmente adatto a contesti in cui è necessario integrare nuove sorgenti
di dati o nuovi canali di output in modo rapido e controllato.

Target audience
---------------

Questa documentazione è rivolta a sviluppatori e system integrator che desiderano
configurare, estendere o integrare Apps Logging App all'interno di sistemi
distribuiti esistenti.
