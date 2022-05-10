Sposób użycia:
1) przenieś Transactions2.java do cp1/demo/
2) skompiluj program
3) odpal z plikami *.in przekierowanymi na wejście

Oczekiwany efekt:
program cośtam wyświetla i w miarę szybko kończy napisem test_passed nie rzucając po drodze żadnych assertów.

Powinien działać raczej deterministycznie pod warunkiem, że czas potrzebny do zajęcia dostępnego zasobu przekracza w waszej implementacji 100ms.
Nie sprawdza działania współbieżności waszej implementacji, jedynie wykrywanie deadlockow.
Testów z wykorzystaniem interrupt jeszcze nie ma, możesz napisać własne.

Sposób działania plików in:
liczba w pierwszej lini określa liczbę zasobów w systemie
liczba w drugiej lini określa liczbę wątków w systemie
po czym następuje ciąg komend (wielkość liter nie jest rozróżniana)
:) thread id ...
    przekazuje do wątku id komendę ...
:) resourceInfo
    wyświetla aktualną wartość zasobów
:) assertNoBusy id
    sprawdza czy wątek o zadanym id nie czeka aktualnie na zasób
:) assertBusy id
    sprawdza czy wątek o zadanym id czeka aktualnie na zasób
:) assertResources r1 r2 ... rn
    sprawdza czy wartość zasobów odpowiada kolejno r1, r2, ..., rn
:) sleep n
    usypia wątek główny na n milisekund
    (używane w celu zapewnienia, że akcje, które powinny trwać krótko,
    powinny się już wykonać np. uzyskanie dostępu do niezajętego zasobu)
:) advanceTime
    zwiększa wartość zwracaną przez localTimeProvider
:) timeInfo
    wyświetla aktualny czas localTimeProvider
:) interrupt id
    przerywa wątek o zadanym id

linie puste i zaczynające się od # są ignorowane

poprawne komendy przekazane do wątku mają postać
:) status
    wyświetla informacje o wątku
:) transaction
    rozpoczyna transakcję
:) rollback
    cofa transakcje
:) commit
    zatwierdza transakcję
:) increase id
    wykonuje operacje zwiększenia wartości zasobu id o 1
:) decrease id
    wykonuje operacje zmniejszenia wartości zasobu id o 1
:) error id
    wykonuje operacje na zasobie id, która zawsze kończy się błędem wykonania
