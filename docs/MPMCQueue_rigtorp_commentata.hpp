/*
Copyright (c) 2020 Erik Rigtorp <erik@rigtorp.se>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

// Evita inclusioni multiple di questo header nella stessa unita di compilazione.
#pragma once

// Operazioni atomiche e ordini di memoria.
#include <atomic>
// Supporto ad assert; in questo frammento non viene usato.
#include <cassert>
// Tipi size_t/ptrdiff_t e macro offsetof per gli offset dei membri.
#include <cstddef> // offsetof
// Limiti numerici usati per prevenire overflow nella dimensione allocata.
#include <limits>
// Allocator standard e utilita di gestione della memoria.
#include <memory>
// Placement new, eccezioni di allocazione e costante di interferenza hardware.
#include <new> // std::hardware_destructive_interference_size
// Eccezioni come invalid_argument.
#include <stdexcept>

// Il ramo seguente serve se manca il supporto a new per tipi con allineamento esteso.
#ifndef __cpp_aligned_new
// Seleziona il ramo Windows.
#ifdef _WIN32
// Dichiara le funzioni Windows di allocazione allineata.
#include <malloc.h> // _aligned_malloc
// Seleziona il ramo non Windows.
#else
// Dichiara posix_memalign e free.
#include <stdlib.h> // posix_memalign
// Chiude la selezione della piattaforma.
#endif
// Chiude il controllo sul supporto a new allineato.
#endif

// Namespace pubblico della libreria.
namespace rigtorp {
// Namespace che contiene i dettagli della coda.
namespace mpmc {
// Usa la costante standard quando disponibile, escludendo esplicitamente Apple.
#if defined(__cpp_lib_hardware_interference_size) && !defined(__APPLE__)
// Definisce una costante interna risolta a compilazione.
static constexpr size_t hardwareInterferenceSize =
    // Distanza suggerita dalla libreria standard per evitare interferenza distruttiva; non e una rilevazione runtime.
    std::hardware_destructive_interference_size;
// Ramo alternativo se la costante non viene usata.
#else
// Ripiego: assume una distanza di 64 byte.
static constexpr size_t hardwareInterferenceSize = 64;
// Chiude la selezione della dimensione di allineamento.
#endif

// Se il compilatore supporta new allineato, usa l'allocator standard.
#if defined(__cpp_aligned_new)
// Alias generico: AlignedAllocator<T> coincide con std::allocator<T>.
template <typename T> using AlignedAllocator = std::allocator<T>;
// Altrimenti definisce un allocator manuale.
#else
// Allocator parametrizzato sul tipo degli oggetti da allocare.
template <typename T> struct AlignedAllocator {
  // Dichiara il tipo degli elementi gestiti, come richiesto all'allocator.
  using value_type = T;

  // Riserva memoria per n elementi T senza costruirli.
  T *allocate(std::size_t n) {
    // Controlla che n * sizeof(T) sia rappresentabile in size_t.
    if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
      // Segnala una dimensione di array eccessiva.
      throw std::bad_array_new_length();
    // Chiude il blocco corrente.
    }
// Allocazione specifica per Windows.
#ifdef _WIN32
    // Alloca n * sizeof(T) byte rispettando alignof(T) e converte il puntatore in T*.
    auto *p = static_cast<T *>(_aligned_malloc(sizeof(T) * n, alignof(T)));
    // Controlla il fallimento dell'allocazione Windows.
    if (p == nullptr) {
      // Segnala che non e stato possibile allocare memoria.
      throw std::bad_alloc();
    // Chiude il blocco corrente.
    }
// Allocazione per piattaforme POSIX.
#else
    // Puntatore che verra valorizzato da posix_memalign.
    T *p;
    // Passa l'indirizzo del puntatore di uscita e l'allineamento richiesto.
    if (posix_memalign(reinterpret_cast<void **>(&p), alignof(T),
                       // Completa la chiamata con la dimensione in byte; zero indica successo.
                       sizeof(T) * n) != 0) {
      // Segnala il fallimento di posix_memalign.
      throw std::bad_alloc();
    // Chiude il blocco corrente.
    }
// Chiude il ramo della piattaforma.
#endif
    // Restituisce memoria allineata ancora priva di oggetti T costruiti.
    return p;
  // Chiude il blocco corrente.
  }

  // Libera memoria; il parametro numerico senza nome non viene usato.
  void deallocate(T *p, std::size_t) {
// Seleziona il rilascio Windows.
#ifdef _WIN32
    // Libera usando la funzione corrispondente a _aligned_malloc.
    _aligned_free(p);
// Seleziona il rilascio POSIX.
#else
    // Libera la memoria allocata da posix_memalign.
    free(p);
// Chiude la selezione della funzione di rilascio.
#endif
  // Chiude il blocco corrente.
  }
// Chiude la definizione del tipo.
};
// Chiude la definizione alternativa dell'allocator.
#endif

// Slot generico: combina uno stato atomico con memoria grezza per un T.
template <typename T> struct Slot {
  // Distruttore dello slot, che non deve propagare eccezioni.
  ~Slot() noexcept {
    // Legge implicitamente l'atomico; bit basso a 1 significa che esiste ancora un T nello slot.
    if (turn & 1) {
      // Distrugge il T residuo; la distruzione della coda richiede assenza di accessi concorrenti.
      destroy();
    // Chiude il blocco corrente.
    }
  // Chiude il blocco corrente.
  }

  // Costruisce T da un numero variabile di argomenti con forwarding references.
  template <typename... Args> void construct(Args &&...args) noexcept {
    // Controllo a compilazione: la costruzione di T con questi argomenti non deve lanciare.
    static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
                  // Messaggio del controllo statico sulla costruzione.
                  "T must be nothrow constructible with Args&&...");
    // Placement new: costruisce T nella memoria gia riservata, preservando le categorie degli argomenti.
    new (&storage) T(std::forward<Args>(args)...);
  // Chiude il blocco corrente.
  }

  // Termina la vita di T senza deallocare la memoria dello slot.
  void destroy() noexcept {
    // Richiede a compilazione un distruttore di T che non lanci.
    static_assert(std::is_nothrow_destructible<T>::value,
                  // Messaggio del controllo statico sulla distruzione.
                  "T must be nothrow destructible");
    // Interpreta la memoria come T e ne chiama esplicitamente il distruttore.
    reinterpret_cast<T *>(&storage)->~T();
  // Chiude il blocco corrente.
  }

  // Espone il T esistente come riferimento rvalue; la funzione non trasferisce da sola il valore.
  T &&move() noexcept { return reinterpret_cast<T &&>(storage); }

  // Align to avoid false sharing between adjacent slots
  // Allinea lo slot alla distanza scelta e inizializza il contatore di stato a zero.
  alignas(hardwareInterferenceSize) std::atomic<size_t> turn = {0};
  // Memoria con dimensione e allineamento sufficienti per T; non costruisce un T.
  typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
// Chiude la definizione del tipo.
};

// Coda parametrizzata su T e su un allocator di Slot<T>, con allocator allineato predefinito.
template <typename T, typename Allocator = AlignedAllocator<Slot<T>>>
// Definizione della classe della coda.
class Queue {
// Inizio della sezione privata.
private:
  // Richiede almeno una forma di assegnazione di T che non lanci: copia...
  static_assert(std::is_nothrow_copy_assignable<T>::value ||
                    // ...oppure spostamento; la forma realmente selezionata deve comunque rispettare noexcept.
                    std::is_nothrow_move_assignable<T>::value,
                // Messaggio del controllo statico sull'assegnazione.
                "T must be nothrow copy or move assignable");

  // Richiede che T possa essere distrutto senza eccezioni.
  static_assert(std::is_nothrow_destructible<T>::value,
                // Messaggio del controllo sul distruttore.
                "T must be nothrow destructible");

// Inizio dell'interfaccia pubblica.
public:
  // Costruttore explicit: impedisce conversioni implicite da capacita a coda.
  explicit Queue(const size_t capacity,
                 // Allocator ricevuto per riferimento costante, con valore predefinito.
                 const Allocator &allocator = Allocator())
      // Inizializza capacita e allocator, e azzera i contatori dei ticket.
      : capacity_(capacity), allocator_(allocator), head_(0), tail_(0) {
    // La capacita deve essere almeno uno; non deve essere una potenza di due.
    if (capacity_ < 1) {
      // Segnala una capacita nulla.
      throw std::invalid_argument("capacity < 1");
    // Chiude il blocco corrente.
    }
    // Allocate one extra slot to prevent false sharing on the last slot
    // Alloca capacita + 1 slot: quello aggiuntivo serve da margine, non aumenta la capacita logica.
    slots_ = allocator_.allocate(capacity_ + 1);
    // Allocators are not required to honor alignment for over-aligned types
    // (see http://eel.is/c++draft/allocator.requirements#10) so we verify
    // alignment here
    // Verifica che l'indirizzo iniziale sia multiplo dell'allineamento richiesto agli slot.
    if (reinterpret_cast<size_t>(slots_) % alignof(Slot<T>) != 0) {
      // Rilascia subito l'allocazione se l'allineamento e errato.
      allocator_.deallocate(slots_, capacity_ + 1);
      // Segnala che l'allocazione non soddisfa il requisito.
      throw std::bad_alloc();
    // Chiude il blocco corrente.
    }
    // Visita solo i capacity_ slot effettivamente usati.
    for (size_t i = 0; i < capacity_; ++i) {
      // Costruisce uno Slot nella memoria allocata: turn parte da zero e T non viene ancora costruito.
      new (&slots_[i]) Slot<T>();
    // Chiude il blocco corrente.
    }
    // Avvia un controllo statico sull'allineamento degli slot.
    static_assert(
        // Richiede che alignof(Slot<T>) coincida con la distanza scelta.
        alignof(Slot<T>) == hardwareInterferenceSize,
        // Messaggio del controllo sull'allineamento.
        "Slot must be aligned to cache line boundary to prevent false sharing");
    // Richiede che la dimensione dello slot sia un multiplo della distanza scelta.
    static_assert(sizeof(Slot<T>) % hardwareInterferenceSize == 0,
                  // Prima parte del messaggio relativo alla dimensione degli slot.
                  "Slot size must be a multiple of cache line size to prevent "
                  // Seconda parte della stringa, concatenata alla precedente dal compilatore.
                  "false sharing between adjacent slots");
    // Richiede che la dimensione della coda sia un multiplo della distanza scelta.
    static_assert(sizeof(Queue) % hardwareInterferenceSize == 0,
                  // Prima parte del messaggio relativo alla dimensione della coda.
                  "Queue size must be a multiple of cache line size to "
                  // Seconda parte del messaggio relativo a code adiacenti.
                  "prevent false sharing between adjacent queues");
    // Avvia il controllo sulla distanza dei contatori.
    static_assert(
        // Calcola la differenza degli offset in byte tra tail_ e head_.
        offsetof(Queue, tail_) - offsetof(Queue, head_) ==
            // La distanza deve essere esattamente hardwareInterferenceSize byte.
            static_cast<std::ptrdiff_t>(hardwareInterferenceSize),
        // Messaggio del controllo relativo ai contatori.
        "head and tail must be a cache line apart to prevent false sharing");
  // Chiude il blocco corrente.
  }

  // Distruttore della coda: deve essere eseguito dopo la fine degli accessi concorrenti.
  ~Queue() noexcept {
    // Visita tutti gli slot inizializzati.
    for (size_t i = 0; i < capacity_; ++i) {
      // Distrugge lo slot, che a sua volta distrugge l'eventuale T presente.
      slots_[i].~Slot();
    // Chiude il blocco corrente.
    }
    // Libera l'intero blocco, incluso lo slot aggiuntivo mai costruito.
    allocator_.deallocate(slots_, capacity_ + 1);
  // Chiude il blocco corrente.
  }

  // non-copyable and non-movable
  // Vieta la costruzione per copia; il distruttore e questi membri impediscono anche il move implicito.
  Queue(const Queue &) = delete;
  // Vieta l'assegnazione per copia.
  Queue &operator=(const Queue &) = delete;

  // Inserimento in-place con attesa: costruisce T dagli argomenti ricevuti.
  template <typename... Args> void emplace(Args &&...args) noexcept {
    // Richiede costruzione senza eccezioni, essenziale dopo aver prenotato un ticket.
    static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
                  // Messaggio del requisito sulla costruzione.
                  "T must be nothrow constructible with Args&&...");
    // Assegna un ticket unico al produttore e incrementa head_; ordine predefinito seq_cst.
    auto const head = head_.fetch_add(1);
    // Ottiene un riferimento allo slot indicato dal ticket modulo capacita.
    auto &slot = slots_[idx(head)];
    // Attende con letture ACQUIRE lo stato pari previsto per questa generazione.
    while (turn(head) * 2 != slot.turn.load(std::memory_order_acquire))
      // Corpo vuoto del while: attesa attiva senza sleep, yield o timeout.
      ;
    // Costruisce T nello slot riservato, preservando lvalue/rvalue degli argomenti.
    slot.construct(std::forward<Args>(args)...);
    // Pubblica T con RELEASE e stato dispari, pronto per il consumatore corrispondente.
    slot.turn.store(turn(head) * 2 + 1, std::memory_order_release);
  // Chiude il blocco corrente.
  }

  // Tentativo di costruzione in-place: non attende che uno slot occupato si liberi, ma puo riprovare per contesa.
  template <typename... Args> bool try_emplace(Args &&...args) noexcept {
    // Richiede che la costruzione con questi argomenti non lanci.
    static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
                  // Messaggio del requisito sulla costruzione.
                  "T must be nothrow constructible with Args&&...");
    // Legge un candidato ticket di scrittura senza prenotarlo.
    auto head = head_.load(std::memory_order_acquire);
    // Ciclo di tentativi senza un limite fisso di iterazioni.
    for (;;) {
      // Identifica lo slot del candidato ticket.
      auto &slot = slots_[idx(head)];
      // Verifica che lo stato sia quello libero della generazione richiesta.
      if (turn(head) * 2 == slot.turn.load(std::memory_order_acquire)) {
        // CAS prenota il ticket; se fallisce aggiorna head al valore osservato, ordine predefinito seq_cst.
        if (head_.compare_exchange_strong(head, head + 1)) {
          // Solo il produttore che vince la CAS costruisce T.
          slot.construct(std::forward<Args>(args)...);
          // Pubblica il dato costruito con RELEASE e stato dispari.
          slot.turn.store(turn(head) * 2 + 1, std::memory_order_release);
          // Segnala successo.
          return true;
        // Chiude il blocco corrente.
        }
      // Ramo eseguito quando lo slot candidato non e pronto.
      } else {
        // Conserva il ticket candidato prima di rileggere il contatore.
        auto const prevHead = head;
        // Rilegge head_ per verificare se la posizione candidata e cambiata.
        head = head_.load(std::memory_order_acquire);
        // Se il contatore e invariato, lo slot indisponibile resta il prossimo candidato.
        if (head == prevHead) {
          // Segnala fallimento senza aver costruito T o consumato un ticket.
          return false;
        // Chiude il blocco corrente.
        }
      // Chiude il blocco corrente.
      }
    // Chiude il blocco corrente.
    }
  // Chiude il blocco corrente.
  }

  // Inserisce copiando da un riferimento costante a T.
  void push(const T &v) noexcept {
    // Richiede che il costruttore di copia non lanci.
    static_assert(std::is_nothrow_copy_constructible<T>::value,
                  // Messaggio del requisito sulla copia.
                  "T must be nothrow copy constructible");
    // Delega la costruzione allo stesso algoritmo di emplace.
    emplace(v);
  // Chiude il blocco corrente.
  }

  // Inizio dell'overload generico di push, parametrizzato sul tipo P.
  template <typename P,
            // SFINAE: abilita l'overload soltanto se esiste il tipo prodotto da enable_if.
            typename = typename std::enable_if<
                // La condizione e che T sia costruibile da P&& senza eccezioni.
                std::is_nothrow_constructible<T, P &&>::value>::type>
  // Riceve una forwarding reference: puo rappresentare lvalue o rvalue in base a P.
  void push(P &&v) noexcept {
    // Inoltra l'argomento a emplace preservandone la categoria.
    emplace(std::forward<P>(v));
  // Chiude il blocco corrente.
  }

  // Tentativo di inserimento per copia: restituisce bool.
  bool try_push(const T &v) noexcept {
    // Richiede copia senza eccezioni.
    static_assert(std::is_nothrow_copy_constructible<T>::value,
                  // Messaggio del requisito sul costruttore di copia.
                  "T must be nothrow copy constructible");
    // Delega a try_emplace e ne restituisce il risultato.
    return try_emplace(v);
  // Chiude il blocco corrente.
  }

  // Inizio dell'overload generico di try_push.
  template <typename P,
            // Parametro template usato per abilitare o scartare l'overload.
            typename = typename std::enable_if<
                // Richiede costruzione di T da P&& senza eccezioni.
                std::is_nothrow_constructible<T, P &&>::value>::type>
  // Tentativo di inserimento con forwarding reference.
  bool try_push(P &&v) noexcept {
    // Inoltra il valore a try_emplace; in caso di false non viene costruito alcun T.
    return try_emplace(std::forward<P>(v));
  // Chiude il blocco corrente.
  }

  // Estrae assegnando a un T gia esistente; attende fino alla disponibilita.
  void pop(T &v) noexcept {
    // Assegna un ticket unico al consumatore incrementando tail_, con ordine predefinito seq_cst.
    auto const tail = tail_.fetch_add(1);
    // Identifica lo slot associato al ticket di lettura.
    auto &slot = slots_[idx(tail)];
    // Attende lo stato dispari della generazione richiesta; ACQUIRE osserva la pubblicazione del produttore.
    while (turn(tail) * 2 + 1 != slot.turn.load(std::memory_order_acquire))
      // Corpo vuoto del while: il thread continua a eseguire letture atomiche.
      ;
    // Assegna a v dal riferimento rvalue al T nello slot: normalmente usa l'assegnazione per move.
    v = slot.move();
    // Distrugge l'oggetto sorgente, che esiste ancora anche dopo un move.
    slot.destroy();
    // Pubblica con RELEASE lo stato pari della generazione successiva, permettendo il riuso.
    slot.turn.store(turn(tail) * 2 + 2, std::memory_order_release);
  // Chiude il blocco corrente.
  }

  // Tenta di estrarre in v; in caso di false v non viene modificato da questo metodo.
  bool try_pop(T &v) noexcept {
    // Legge un candidato ticket di lettura senza prenotarlo.
    auto tail = tail_.load(std::memory_order_acquire);
    // Ripete se altri consumatori cambiano la posizione candidata.
    for (;;) {
      // Identifica lo slot del ticket candidato.
      auto &slot = slots_[idx(tail)];
      // Controlla che sia pubblicato il dato della generazione attesa.
      if (turn(tail) * 2 + 1 == slot.turn.load(std::memory_order_acquire)) {
        // CAS prenota la lettura; se fallisce aggiorna tail al valore osservato.
        if (tail_.compare_exchange_strong(tail, tail + 1)) {
          // Trasferisce il contenuto tramite assegnazione al parametro di uscita.
          v = slot.move();
          // Distrugge il T rimasto nello slot dopo l'assegnazione.
          slot.destroy();
          // Rende riutilizzabile lo slot per la generazione successiva con RELEASE.
          slot.turn.store(turn(tail) * 2 + 2, std::memory_order_release);
          // Segnala estrazione riuscita.
          return true;
        // Chiude il blocco corrente.
        }
      // Ramo in cui il dato del candidato non e ancora disponibile.
      } else {
        // Memorizza il ticket prima di rileggere tail_.
        auto const prevTail = tail;
        // Rilegge il prossimo ticket di lettura.
        tail = tail_.load(std::memory_order_acquire);
        // Se il contatore e invariato, non c'e un nuovo candidato da provare.
        if (tail == prevTail) {
          // Segnala fallimento; questo non e una fotografia atomica di tutta la coda.
          return false;
        // Chiude il blocco corrente.
        }
      // Chiude il blocco corrente.
      }
    // Chiude il blocco corrente.
    }
  // Chiude il blocco corrente.
  }

  /// Returns the number of elements in the queue.
  /// The size can be negative when the queue is empty and there is at least one
  /// reader waiting. Since this is a concurrent queue the size is only a best
  /// effort guess until all reader and writer threads have been joined.
  // Stima la differenza tra ticket di scrittura e lettura; il tipo signed permette risultati negativi.
  ptrdiff_t size() const noexcept {
    // TODO: How can we deal with wrapped queue on 32bit?
    // Legge head_ con RELAXED e sottrae la lettura separata di tail_.
    return static_cast<ptrdiff_t>(head_.load(std::memory_order_relaxed) -
                                  // Completa la sottrazione unsigned e la converte a ptrdiff_t: wraparound e portabilita richiedono attenzione.
                                  tail_.load(std::memory_order_relaxed));
  // Chiude il blocco corrente.
  }

  /// Returns true if the queue is empty.
  /// Since this is a concurrent queue this is only a best effort guess
  /// until all reader and writer threads have been joined.
  // Stima se la coda e vuota; non usare il risultato per prenotare un'operazione.
  bool empty() const noexcept { return size() <= 0; }

// Inizio delle funzioni ausiliarie private.
private:
  // Indice fisico = resto della divisione del ticket per la capacita.
  constexpr size_t idx(size_t i) const noexcept { return i % capacity_; }

  // Generazione = quoziente intero della divisione del ticket per la capacita.
  constexpr size_t turn(size_t i) const noexcept { return i / capacity_; }

// Inizio dei dati membri privati.
private:
  // Capacita fissata alla costruzione e non modificabile.
  const size_t capacity_;
  // Puntatore al buffer di slot allocato manualmente.
  Slot<T> *slots_;
// Controlla il supporto all'attributo che puo evitare spazio per allocator vuoti.
#if defined(__has_cpp_attribute) && __has_cpp_attribute(no_unique_address)
  // L'attributo consente all'allocator di condividere spazio se possibile, senza garantirlo.
  Allocator allocator_ [[no_unique_address]];
// Ramo di compatibilita senza attributo.
#else
  // Memorizza l'allocator come membro normale.
  Allocator allocator_;
// Chiude il controllo sull'attributo.
#endif

  // Align to avoid false sharing between head_ and tail_
  // Contatore dei ticket di scrittura, allineato alla distanza scelta.
  alignas(hardwareInterferenceSize) std::atomic<size_t> head_;
  // Contatore dei ticket di lettura, allineato separatamente per evitare false sharing.
  alignas(hardwareInterferenceSize) std::atomic<size_t> tail_;
// Chiude la definizione del tipo.
};
// Chiude il namespace dei dettagli mpmc.
} // namespace mpmc

// Parametri template dell'alias pubblico.
template <typename T,
          // Allocator predefinito per gli slot del tipo T.
          typename Allocator = mpmc::AlignedAllocator<mpmc::Slot<T>>>
// Alias pubblico: rigtorp::MPMCQueue<T> indica mpmc::Queue<T, Allocator>.
using MPMCQueue = mpmc::Queue<T, Allocator>;

// Chiude il namespace pubblico rigtorp.
} // namespace rigtorp
