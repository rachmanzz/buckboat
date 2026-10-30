package document

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func NewRouter() http.Handler {
	r := chi.NewRouter()
	unimplemented := http.HandlerFunc(notImplemented)
	r.Handle("/v1", unimplemented)
	r.Handle("/v1/*", unimplemented)
	return r
}

func notImplemented(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "Document API is not implemented in this skeleton", http.StatusNotImplemented)
}
