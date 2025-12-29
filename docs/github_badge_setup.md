# GitHub Badge Setup Guide

This guide explains how to set up the FDA compatibility status badge for your maude_db repository.

## What is the Compatibility Badge?

The compatibility badge shows the current status of the FDA MAUDE website compatibility:

![FDA Site](https://img.shields.io/badge/FDA%20Site-compatible-brightgreen) - Website is compatible
![FDA Site](https://img.shields.io/badge/FDA%20Site-incompatible-red) - Website has breaking changes

The badge is automatically updated daily by GitHub Actions.

## Setup Steps

### 1. Create a GitHub Gist for the Badge

1. Go to https://gist.github.com/
2. Create a new **secret** gist (important: must be secret for the badge to work)
3. Name the file: `maude_db_fda_compatibility.json`
4. Add this initial content:
   ```json
   {
     "schemaVersion": 1,
     "label": "FDA Site",
     "message": "checking...",
     "color": "yellow"
   }
   ```
5. Click "Create secret gist"
6. **Copy the Gist ID** from the URL (e.g., if URL is `https://gist.github.com/username/abc123def456`, the ID is `abc123def456`)

### 2. Create a GitHub Personal Access Token

1. Go to GitHub Settings → Developer settings → Personal access tokens → Tokens (classic)
   - Direct link: https://github.com/settings/tokens
2. Click "Generate new token (classic)"
3. Give it a descriptive name: `maude_db_badge_updater`
4. Set expiration (recommend: 1 year, then set a reminder to renew)
5. Select scopes:
   - ✅ `gist` (required to update the gist)
6. Click "Generate token"
7. **Copy the token immediately** (you won't see it again)

### 3. Add Token as Repository Secret

1. Go to your maude_db repository on GitHub
2. Click Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Name: `GIST_SECRET`
5. Value: Paste the personal access token
6. Click "Add secret"

### 4. Update the Workflow File

1. Open `.github/workflows/fda_compatibility_check.yml`
2. Find this line:
   ```yaml
   gistID: YOUR_GIST_ID_HERE  # Replace with your gist ID
   ```
3. Replace `YOUR_GIST_ID_HERE` with your actual Gist ID from step 1
4. Commit and push the change

### 5. Test the Workflow

1. Go to your repository on GitHub
2. Click Actions tab
3. Click "FDA Compatibility Check" workflow
4. Click "Run workflow" button
5. Wait for it to complete (takes ~30 seconds)
6. Check that the badge in your gist was updated

### 6. Add Badge to README

Add this markdown to your README.md:

```markdown
[![FDA Site Compatibility](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/USERNAME/GIST_ID/raw/maude_db_fda_compatibility.json)](https://github.com/USERNAME/maude_db/actions/workflows/fda_compatibility_check.yml)
```

Replace:
- `USERNAME` with your GitHub username
- `GIST_ID` with your Gist ID from step 1

## How It Works

1. **GitHub Actions** runs the compatibility check:
   - Daily at 6 AM UTC (automated)
   - When you push changes to `maude_db.py` or the workflow
   - When you manually trigger it

2. **The check** verifies:
   - FDA base URL is accessible
   - File naming patterns are correct
   - Files are available for download
   - Downloaded files can be parsed correctly

3. **Badge updates** automatically:
   - Green "compatible" if all checks pass
   - Red "incompatible" if any critical check fails

4. **Issue creation** if incompatible:
   - Automatically creates a GitHub issue with details
   - Includes error messages and check results
   - Closes the issue when compatibility is restored

## Customization

### Change Check Frequency

Edit the cron schedule in `.github/workflows/fda_compatibility_check.yml`:

```yaml
schedule:
  - cron: '0 6 * * *'  # Daily at 6 AM UTC
```

Examples:
- Every 12 hours: `'0 */12 * * *'`
- Weekly (Mondays): `'0 6 * * 1'`
- Twice daily: `'0 6,18 * * *'`

### Customize Badge Appearance

You can customize the badge by editing the workflow step "Create compatibility badge":

```yaml
- name: Create compatibility badge
  uses: schneegans/dynamic-badges-action@v1.7.0
  with:
    auth: ${{ secrets.GIST_SECRET }}
    gistID: YOUR_GIST_ID
    filename: maude_db_fda_compatibility.json
    label: FDA MAUDE Site    # Change the label text
    message: ${{ steps.parse_results.outputs.badge_message }}
    color: ${{ steps.parse_results.outputs.badge_color }}
    style: flat-square       # Add this for different badge style
```

Badge styles:
- `flat` (default)
- `flat-square`
- `plastic`
- `for-the-badge`

## Troubleshooting

### Badge shows "invalid"

- Check that your Gist is **secret** (not public)
- Verify the Gist ID in the workflow is correct
- Make sure the filename matches: `maude_db_fda_compatibility.json`

### Badge not updating

- Check that the GitHub Action ran successfully
- Verify the `GIST_SECRET` is set correctly in repository secrets
- Check the Action logs for errors

### Permission errors

- Make sure the personal access token has `gist` scope
- Verify the token hasn't expired
- Regenerate token if necessary and update the secret

### Workflow failing

- Check the Actions tab for error logs
- Ensure Python dependencies are installed correctly
- Verify FDA website is accessible from GitHub Actions runners

## Manual Testing

You can run the compatibility check manually:

```bash
# Using Makefile (recommended)
make check-fda           # Full check with test download
make check-fda-quick     # Quick check (HEAD requests only)

# Or run the script directly
python archive_tools/check_fda_compatibility.py
python archive_tools/check_fda_compatibility.py --quick
python archive_tools/check_fda_compatibility.py --json
```

## Security Notes

1. **Never commit tokens to the repository** - always use GitHub Secrets
2. **Use secret gists** - public gists may not work with the badge action
3. **Limit token scope** - only grant the `gist` permission
4. **Rotate tokens regularly** - set a reminder to regenerate annually
5. **Monitor Action runs** - review logs if badge shows unexpected status

## Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Dynamic Badges Action](https://github.com/Schneegans/dynamic-badges-action)
- [Shields.io Badge Styles](https://shields.io/)
- [GitHub Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets)
