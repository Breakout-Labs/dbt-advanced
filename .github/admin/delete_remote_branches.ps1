# Fetch the latest remote branches
git fetch --prune

# Get all remote branches, remove 'origin/' prefix, and exclude HEAD
$remoteBranches = git branch -r | ForEach-Object { $_.Trim() -replace '^origin/', '' } | Where-Object { $_ -ne 'HEAD' }

foreach ($branch in $remoteBranches) {
    if ($branch -notlike 'admin*' -and $branch -ne 'main') {
        Write-Host "Deleting remote branch: $branch"
        git push origin --delete $branch
    }
}

Write-Host "Remote branch cleanup completed!" 

#.\.github\admin\delete_remote_branches.ps1
