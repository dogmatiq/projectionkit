package patchprojection

// Stream is used to read projection state in the form of a stream of full state
// transfers and patches.
type Stream struct {
}

// func (s *Stream) Consume(context.Context, uint64) (*Cursor, error) {
// 	panic("not implemented")
// }

// func (s *Stream) ConsumeTail(context.Context) (*Cursor, error) {
// 	panic("not implemented")
// }

// Cursor consumes updates from a stream.
// type Cursor struct {
// }

// // Next blocks until the next update is available or ctx is canceled.
// func (c *Cursor) Next(ctx context.Context) (Update, error) {
// 	panic("not implemented")
// }

// // Close closes the cursor.
// func (c *Cursor) Close() error {
// 	return nil
// }

// // An Update is a change to the state of a projection. It can be represented as
// // either a full state transfer, or a patch.
// type Update struct {
// 	offset uint64
// 	state  State
// 	patch  Patch
// }

// // State returns the projection state if the update is a full state transfer.
// //
// // If the update is a patch ok is false and s is nil.
// func (u Update) State() (s State, ok bool) {
// 	return u.state, u.state != nil
// }

// // Patch returns a patch for the projection state if the update is a patch.
// //
// // If the update is a full state transfer ok is false and p is nil.
// func (u Update) Patch() (p Patch, ok bool) {
// 	return u.patch, u.patch != nil
// }
