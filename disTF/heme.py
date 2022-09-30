def load_heme_tfs():
    heme_tfs_file = 'heme_tfs.txt'
    with open(heme_tfs_file, 'r') as file:
        heme_tfs = file.read().splitlines()
    
    return heme_tfs

def load_heme_cells():
    heme_cells_file = "heme_cells.txt"
    with open(heme_cells_file, 'r') as file:
        heme_cells = file.read().splitlines()
    
    return heme_cells
