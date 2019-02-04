PARTICLE_IDS = {
    1: 'gamma',
    2: 'electron',
    3: 'positron',
    5: 'mu_plus',
    6: 'mu_minus',
    13: 'neutron',
    14: 'proton',
    302: 'helium3',
    402: 'helium4',
    5626: 'iron',
}


def primary_id_to_name(primary_id):
    ''' if unknown just return the primary_id as string'''
    return PARTICLE_IDS.get(primary_id, str(primary_id))
