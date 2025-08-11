{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  packages = with pkgs; [
    # Python env (only Python packages go here)
    (python312.withPackages (ps:
      with ps; [
        streamlit
      ]))

    # Non-Python tools from nixpkgs
    samtools
    minimap2
  ];
}
