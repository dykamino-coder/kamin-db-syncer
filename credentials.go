package main

// SQL logins: from the environment (CRED_<NAME>_USER / _PASSWORD, Kubernetes Secret) or entered in
// the UI and stored in MongoDB with the password encrypted (AES-256-GCM, SECRET_KEY). Jobs refer to
// a login by name; the password never leaves the server.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type storedCred struct {
	Name      string    `bson:"_id" json:"name"`
	User      string    `bson:"user" json:"user"`
	PassEnc   string    `bson:"passEnc" json:"-"`
	CreatedAt time.Time `bson:"createdAt" json:"created_at"`
	CreatedBy string    `bson:"createdBy" json:"created_by"`
}

type credInfo struct {
	Name   string `json:"name"`
	User   string `json:"user"`
	Source string `json:"source"` // env | ui
}

var (
	credColl  *mongo.Collection
	credMu    sync.RWMutex
	credCache = map[string]storedCred{}
)

func initCredentials(ctx context.Context, s *Store) error {
	credColl = s.client.Database(s.jobs.Database().Name()).Collection("credentials")
	return reloadCredentials(ctx)
}

func reloadCredentials(ctx context.Context) error {
	cur, err := credColl.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	var all []storedCred
	if err := cur.All(ctx, &all); err != nil {
		return err
	}
	m := map[string]storedCred{}
	for _, c := range all {
		m[strings.ToLower(c.Name)] = c
	}
	credMu.Lock()
	credCache = m
	credMu.Unlock()
	return nil
}

func uiCredential(name string) (storedCred, bool) {
	credMu.RLock()
	c, ok := credCache[strings.ToLower(name)]
	credMu.RUnlock()
	if !ok && credColl != nil { // created on another replica
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if reloadCredentials(ctx) == nil {
			credMu.RLock()
			c, ok = credCache[strings.ToLower(name)]
			credMu.RUnlock()
		}
	}
	return c, ok
}

// credential resolves a login by name: environment first, then logins saved in the UI.
func credential(name string) (user, pass string, ok bool) {
	key := credentialKey(name)
	user, pass = os.Getenv("CRED_"+key+"_USER"), os.Getenv("CRED_"+key+"_PASSWORD")
	if user != "" && pass != "" {
		return user, pass, true
	}
	c, found := uiCredential(name)
	if !found {
		return "", "", false
	}
	p, err := decryptSecret(c.PassEnc)
	if err != nil {
		return "", "", false
	}
	return c.User, p, true
}

func allCredentials() []credInfo {
	var out []credInfo
	for _, n := range credentialNames() {
		u, _, _ := credential(n)
		out = append(out, credInfo{Name: n, User: u, Source: "env"})
	}
	credMu.RLock()
	for _, c := range credCache {
		out = append(out, credInfo{Name: c.Name, User: c.User, Source: "ui"})
	}
	credMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name) })
	return out
}

var credNameRe = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,80}$`)

func (a *API) listCredentials(w http.ResponseWriter, r *http.Request) {
	_ = reloadCredentials(r.Context())
	writeJSON(w, 200, allCredentials())
}

func (a *API) saveCredential(w http.ResponseWriter, r *http.Request) {
	var in struct{ Name, User, Password string }
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		fail(w, 400, err.Error())
		return
	}
	in.Name, in.User = strings.TrimSpace(in.Name), strings.TrimSpace(in.User)
	var errs []string
	if !credNameRe.MatchString(in.Name) {
		errs = append(errs, "name: latin letters, digits, _ . @ - (up to 80 characters)")
	}
	if in.User == "" {
		errs = append(errs, "login: required")
	}
	if in.Password == "" {
		errs = append(errs, "password: required")
	}
	if u, _, _ := credential(in.Name); u != "" && os.Getenv("CRED_"+credentialKey(in.Name)+"_USER") != "" {
		errs = append(errs, "this credential is already defined in the environment (Kubernetes Secret)")
	}
	if len(errs) > 0 {
		fail(w, http.StatusUnprocessableEntity, errs)
		return
	}
	enc, err := encryptSecret(in.Password)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, []string{err.Error()})
		return
	}
	c := storedCred{Name: in.Name, User: in.User, PassEnc: enc, CreatedAt: utcNow(), CreatedBy: uiUser(r)}
	if _, err := credColl.ReplaceOne(r.Context(), bson.M{"_id": c.Name}, c, options.Replace().SetUpsert(true)); !a.ok(w, err) {
		return
	}
	_ = reloadCredentials(r.Context())
	writeJSON(w, 200, credInfo{Name: c.Name, User: c.User, Source: "ui"})
}

func (a *API) deleteCredential(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	jobs, err := a.store.ListJobs(r.Context())
	if !a.ok(w, err) {
		return
	}
	for _, j := range jobs {
		if strings.EqualFold(j.Config.Source.Credential, name) || strings.EqualFold(j.Config.Target.Credential, name) {
			fail(w, http.StatusConflict, "the credential is used by sync \""+j.Name+"\"")
			return
		}
	}
	res, err := credColl.DeleteOne(r.Context(), bson.M{"_id": name})
	if !a.ok(w, err) {
		return
	}
	if res.DeletedCount == 0 {
		fail(w, 404, errors.New("no such credential (credentials from the environment are removed in the Secret)").Error())
		return
	}
	_ = reloadCredentials(r.Context())
	w.WriteHeader(http.StatusNoContent)
}
