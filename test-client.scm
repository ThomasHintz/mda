; author: Thomas Hintz
; email: t@thintz.com
; license: bsd

; Copyright (c) 2012, Thomas Hintz
; All rights reserved.

; Redistribution and use in source and binary forms, with or without
; modification, are permitted provided that the following conditions are met:
;     * Redistributions of source code must retain the above copyright
;       notice, this list of conditions and the following disclaimer.
;     * Redistributions in binary form must reproduce the above copyright
;       notice, this list of conditions and the following disclaimer in the
;       documentation and/or other materials provided with the distribution.
;     * Neither the name of the <organization> nor the
;       names of its contributors may be used to endorse or promote products
;       derived from this software without specific prior written permission.

; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
; ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
; WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
; DISCLAIMED. IN NO EVENT SHALL THOMAS HINTZ BE LIABLE FOR ANY
; DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
; (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
; 	    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
; 	    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
; 	    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
; 	    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

(use zmq)

(define socket (make-socket 'req))
(connect-socket socket "tcp://localhost:4444")

(define (serialize l) (with-output-to-string (lambda () (write l))))
(define (deserialize s) (with-input-from-string s (lambda () (read))))

(define (store-test)
  (send-message socket (serialize '(put "v" "k")))
  (receive-message* socket))

(define (read-test)
  (send-message socket (serialize '(get "k")))
  (receive-message* socket))

;(store-test)
;(read-test)

(define (do-op op)
  (send-message socket (serialize op))
  (deserialize (receive-message* socket)))

(define (put v . k)
  (do-op `(put ,v ,@k)))

(define (get k)
  (do-op `(get ,k)))

(define (many n proc)
  (letrec ((process (lambda (i) (if (> i n) 'done (begin (proc) (process (+ i 1)))))))
    (process 0)))

(define (perf-test n) (many n (lambda () (put "v" "k") (get "k"))))
(define (perf-test2 n) (many n (lambda () (do-op '(+ 1 2)))))