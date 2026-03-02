from pyVmomi import vim

# Testa FileQuery e FileQueryFlags
print("=== vim.FileQueryFlags ===")
fqf = vim.FileQueryFlags
print("Membros:", [m for m in dir(fqf) if not m.startswith('_')])

print("\n=== vim.FileQuery ===")
fq = vim.FileQuery
print("Membros:", [m for m in dir(fq) if not m.startswith('_')])

# Testa criar SearchSpec com matchPattern para .vmdk
print("\n=== SearchSpec com matchPattern + FileQueryFlags ===")
try:
    spec = vim.host.DatastoreBrowser.SearchSpec(
        matchPattern=["*.vmdk", "*.vmx"],
        details=vim.host.DatastoreBrowser.FileInfo.Details(
            fileType=True,
            fileSize=True,
            modification=True,
        ),
        sortFoldersFirst=False,
    )
    print("OK! spec criado:", spec)
except Exception as e:
    print("Erro:", e)

# Testa FileInfo.Details
print("\n=== FileInfo.Details membros ===")
print([m for m in dir(vim.host.DatastoreBrowser.FileInfo.Details) if not m.startswith('_')])
