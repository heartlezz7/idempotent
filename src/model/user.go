package model

// User is your `user` struct. Note the db tag on Name is "name" --
// in the sample you sent both fields were tagged db:"id", which would
// make any struct-scanning library read the id column twice.
type User struct {
	ID      string `json:"id"      db:"id"`
	Name    string `json:"name"    db:"name"`
	Version int    `json:"version" db:"version"` // drives ETag / If-Match
}

// ---- request bodies -------------------------------------------------

type CreateUserReq struct {
	Name string `json:"name" binding:"required"`
}

type PutUserReq struct {
	Name string `json:"name" binding:"required"`
}

// PatchUserReq is a JSON Merge Patch (RFC 7396). Pointer fields let us
// tell "absent" from "present and empty". Assignment, not accumulation,
// so applying it twice is a no-op.
type PatchUserReq struct {
	Name *string `json:"name"`
}

// PatchUserNaiveReq is the anti-pattern: an accumulating operation.
// Applying it twice does NOT produce the same state.
type PatchUserNaiveReq struct {
	AppendName string `json:"append_name"`
}
