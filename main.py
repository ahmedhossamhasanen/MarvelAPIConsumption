from MarvelAPIConsumption import Marvel

if __name__ == '__main__':
    public_key = 'Put Marvel API Public key here!'
    private_key = 'Put Marvel API Private key here!'
    marvel = Marvel(public_key, private_key)
    marvel.run()
