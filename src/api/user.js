import map from 'lodash.map'

import {InvalidParameters} from '../api-errors'

// ===================================================================

export async function create ({email, password, permission}) {
  return (await this.createUser({email, password, permission})).id
}

create.description = 'creates a new user'

create.permission = 'admin'

create.params = {
  email: { type: 'string' },
  password: { type: 'string' },
  permission: { type: 'string', optional: true}
}

// -------------------------------------------------------------------

// Deletes an existing user.
async function delete_ ({id}) {
  if (id === this.session.get('user_id')) {
    throw new InvalidParameters('an user cannot delete itself')
  }

  await this.deleteUser(id)
}

// delete is not a valid identifier.
export {delete_ as delete}

delete_.description = 'deletes an existing user'

delete_.permission = 'admin'

delete_.params = {
  id: { type: 'string' }
}

// -------------------------------------------------------------------

// TODO: remove this function when users are integrated to the main
// collection.
export async function getAll () {
  // Retrieves the users.
  const users = await this._users.get()

  // Filters out private properties.
  return map(users, this.getUserPublicProperties)
}

getAll.description = 'returns all the existing users'

getAll.permission = 'admin'

// -------------------------------------------------------------------

export async function set ({id, email, password, permission}) {
  await this.updateUser(id, {email, password, permission})
}

set.description = 'changes the properties of an existing user'

set.permission = 'admin'

set.params = {
  id: { type: 'string' },
  email: { type: 'string', optional: true },
  password: { type: 'string', optional: true },
  permission: { type: 'string', optional: true }
}
