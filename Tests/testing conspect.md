# Rodzaje testów

* Testy obciążeniowe (Load Tests):

* Mierz czas odpowiedzi na różne operacje (INSERT, UPDATE, DELETE, SELECT).
Zmierz wydajność przy dużej liczbie jednoczesnych użytkowników (concurrency).
Testy wydajnościowe (Performance Tests):

* Zbadaj czas potrzebny na wykonanie zapytań na dużych zbiorach danych.
Użyj różnych zapytań, aby ocenić wydajność indeksów, wyszukiwań, agregacji itp.
Testy skalowalności (Scalability Tests):

* Zbadaj, jak silniki radzą sobie z rosnącą ilością danych i obciążenia.
Oceń, jak efektywnie można dodawać węzły w MongoDB w porównaniu do replikacji w MariaDB.

## Metryki do porównania

Czas odpowiedzi (Response Time):

* Średni czas wykonania zapytań i 99. percentyl czasów odpowiedzi.
* Wydajność transakcji (Transactions Per Second - TPS):
* Liczba transakcji wykonanych na sekundę podczas obciążenia.

## Wykorzystanie zasobów

* Monitorowanie CPU, pamięci RAM, I/O dysku i sieci w czasie testów.
* Czas do pierwszego bajtu (Time to First Byte - TTFB):
* Czas od wysłania zapytania do otrzymania pierwszego bajtu odpowiedzi.
