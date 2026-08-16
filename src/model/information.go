package model

// Information is your `information` struct.
type Information struct {
	ID      string `json:"id"      db:"id"`
	Phone   string `json:"phone"   db:"phone"`
	Age     int    `json:"age"     db:"age"`
	Version int    `json:"version" db:"version"`
}

type CreateInformationReq struct {
	Phone string `json:"phone" binding:"required"`
	Age   int    `json:"age"   binding:"required"`
}

type PatchInformationReq struct {
	Phone *string `json:"phone"`
	Age   *int    `json:"age"`
}

// PatchInformationNaiveReq: `age = age + n`. Never idempotent.
type PatchInformationNaiveReq struct {
	AgeIncrement int `json:"age_increment"`
}
